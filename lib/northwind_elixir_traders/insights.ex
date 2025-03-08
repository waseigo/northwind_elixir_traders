defmodule NorthwindElixirTraders.Insights do
  import Ecto.Query

  alias NorthwindElixirTraders.{
    Repo,
    Product,
    Order,
    OrderDetail,
    Customer,
    Employee,
    Shipper,
    Supplier,
    Category,
    Joins
  }

  @tables [Supplier, Category, Product, OrderDetail, Order, Employee, Shipper, Customer]
  @lhs Enum.slice(@tables, 0..1)
  @rhs Enum.slice(@tables, -3..-1)
  @m_tables @tables -- [Order, OrderDetail]
  @timeout 10_000
  @max_concurrency System.schedulers_online()

  def query_order_details_by_order(order_id) do
    join(OrderDetail, :inner, [od], p in Product, on: od.product_id == p.id)
    |> where([od], od.order_id == ^order_id)
  end

  def query_order_detail_values(order_id) do
    query_order_details_by_order(order_id)
    |> select([od, p], od.quantity * p.price)
  end

  def query_order_total_value(order_id) do
    query_order_details_by_order(order_id)
    |> select([od, p], sum(od.quantity * p.price))
  end

  def calculate_order_value(%Order{id: order_id}), do: calculate_order_value(order_id)

  def calculate_order_value(order_id) when not is_map(order_id) do
    order_id |> query_order_total_value() |> Repo.one()
  end

  def calculate_total_value_of_orders(orders, opts \\ [max_concurrency: @max_concurrency])
      when is_list(orders) and is_list(opts) do
    mc =
      if Keyword.has_key?(opts, :max_concurrency),
        do: Keyword.get(opts, :max_concurrency),
        else: @max_concurrency

    Task.async_stream(orders, &calculate_order_value/1,
      ordered: false,
      timeout: @timeout,
      max_concurrency: mc
    )
    |> Enum.to_list()
    |> Enum.sum_by(&elem(&1, 1))

    # |> Enum.reduce(0, fn {_status, value}, acc -> acc + value end)
    # if not using Enum.sum_by/2 or the backported sum_by/2
  end

  def dollarize(cents) when is_number(cents), do: cents / 100

  # for users of Elixir versions below 1.18.0
  def sum_by(enumerable, mapper)

  def sum_by(list, mapper) when is_list(list) and is_function(mapper, 1) do
    sum_by_list(list, mapper, 0)
  end

  def sum_by(enumerable, mapper) when is_function(mapper, 1) do
    Enum.reduce(enumerable, 0, fn x, acc -> acc + mapper.(x) end)
  end

  defp sum_by_list([], _, acc), do: acc
  defp sum_by_list([h | t], mapper, acc), do: sum_by_list(t, mapper, acc + mapper.(h))

  def list_top_n_customers_by_order_count(n \\ 5) when is_integer(n) do
    Customer
    |> join(:inner, [c], o in assoc(c, :orders))
    |> group_by([c, o], c.id)
    |> select([c, o], %{id: c.id, name: c.name, num_orders: count(o.id)})
    |> order_by([c, o], desc: count(o.id))
    |> limit(^n)
    |> Repo.all()
  end

  def query_orders_by_customer(%Customer{id: customer_id}),
    do: query_orders_by_customer(customer_id)

  def query_orders_by_customer(customer_id) when not is_map(customer_id) do
    from(o in Order,
      join: c in Customer,
      on: o.customer_id == c.id,
      where: o.customer_id == ^customer_id,
      select: o
    )
  end

  def list_customers_by_order_revenue do
    from(s in subquery(query_customers_by_order_revenue()),
      order_by: [desc: s.revenue]
    )
    |> Repo.all()
  end

  def query_customers_by_order_revenue, do: query_entity_by_order_revenue(Customer)
  def query_entity_by_order_revenue(m), do: Joins.p_od_group_and_select(m)
  def query_entity_by_product_quantity(m), do: Joins.p_od_group_and_select(m)
  def query_entity_record_totals(m), do: Joins.p_od_group_and_select(m)

  def query_top_n_customers_by_order_revenue(n \\ 5),
    do: query_top_n_entity_by_order_revenue(Customer, n)

  def query_top_n_entity_by_order_revenue(m, n \\ 5), do: query_top_n_entity_by(m, :revenue, n)

  def query_top_n_entity_by(m, field, n \\ 5)
      when is_integer(n) and n >= 0 and field in [:quantity, :revenue] do
    from(s in subquery(query_entity_record_totals(m)),
      order_by: [desc: field(s, ^field)],
      limit: ^n
    )
  end

  def calculate_top_n_customers_by_order_value(n \\ 5),
    do: calculate_top_n_entity_by_order_value(Customer, n)

  def calculate_top_n_entity_by_order_value(m, n \\ 5),
    do: calculate_top_n_entity_by(m, :revenue, n)

  def calculate_top_n_entity_by(m, field, n \\ 5) do
    if n == 0,
      do: 0,
      else:
        from(s in subquery(query_top_n_entity_by(m, field, n)), select: sum(field(s, ^field)))
        |> Repo.one()
  end

  def count_customers_with_revenues do
    from(s in subquery(query_customers_by_order_revenue()),
      where: s.revenue > 0,
      select: count(s.id)
    )
    |> Repo.one()
  end

  def count_customers_orders(condition \\ :with), do: count_entity_orders(Customer, condition)

  def count_entity_orders(m, condition \\ :with)
      when m in @m_tables and condition in [:with, :without] do
    count_with =
      from(x in m)
      |> join(:inner, [x], o in assoc(x, :orders))
      |> select([x], x.id)
      |> distinct(true)
      |> Repo.aggregate(:count)

    case condition do
      :with -> count_with
      :without -> Repo.aggregate(m, :count) - count_with
    end
  end

  def generate_customer_share_of_revenues_xy, do: generate_entity_share_of_revenues_xy(Customer)
  def generate_entity_share_of_revenues_xy(m), do: generate_entity_share_of_xy(m, :revenue)

  def generate_entity_share_of_xy(m, field) do
    0..count_entity_orders(m, :with)
    |> Task.async_stream(&{&1, calculate_top_n_entity_by(m, field, &1)})
    |> Enum.to_list()
    |> extract_task_results()
    |> normalize_xy()
  end

  def calculate_chunk_area({{x1, y1}, {x2, y2}}) do
    {w, h} = {x2 - x1, y2 - y1}
    w * h * 0.5 + y1 * w
  end

  def normalize_xy(xyl) when is_list(xyl) do
    {mxn, mxr} =
      xyl |> Enum.reduce({0, 0}, fn {n, r}, {mxn, mxr} -> {max(n, mxn), max(r, mxr)} end)

    xyl |> Enum.map(fn {n, r} -> {n / mxn, r / mxr} end)
  end

  def extract_task_results(r) when is_list(r), do: Enum.map(r, &elem(&1, 1))

  def calculate_gini_coeff(xyl) when is_list(xyl) do
    xyl
    |> then(&Enum.zip(&1, tl(&1)))
    |> Enum.reduce(0.0, fn c, acc -> acc + calculate_chunk_area(c) end)
    |> Kernel.-(0.5)
    |> Kernel.*(2)
  end

  def gini(m, field), do: generate_entity_share_of_xy(m, field) |> calculate_gini_coeff()

  def calculate_relative_revenue_share_of_entity_rows(m),
    do: calculate_relative_share_of_entity_rows(m, :revenue)

  def calculate_relative_share_of_entity_rows(m, field) do
    data =
      from(s in subquery(query_entity_record_totals(m)),
        order_by: [desc: field(s, ^field)]
      )
      |> Repo.all()

    total = Enum.sum_by(data, &Map.get(&1, field))

    Enum.map(data, fn x ->
      %{id: x.id, name: x.name, share: x[field] / total}
    end)
  end

  def revenue_share_total_trivial_many(m, q \\ 0.8), do: share_total_trivial_many(m, :revenue, q)

  def share_total_trivial_many(m, field, q \\ 0.8) do
    calculate_relative_share_of_entity_rows(m, field)
    |> Enum.reverse()
    |> helper_vital_trivial(m, q)
  end

  def revenue_share_total_vital_few(m, q \\ 0.2), do: share_total_vital_few(m, :revenue, q)

  def share_total_vital_few(m, field, q \\ 0.2),
    do: calculate_relative_share_of_entity_rows(m, field) |> helper_vital_trivial(m, q)

  def helper_vital_trivial(data, m, q)
      when is_list(data) and m in @m_tables and is_number(q) and q > 0 and q <= 1 do
    n = m |> count_entity_orders() |> Kernel.*(q) |> round()
    data |> Enum.take(n) |> Enum.sum_by(& &1.share)
  end

  def to_utc_datetime!(iso_date = %Date{}, :start),
    do: DateTime.new!(iso_date, ~T[00:00:00], "Etc/UTC")

  def to_utc_datetime!(iso_date = %Date{}, :end),
    do: DateTime.new!(iso_date, ~T[23:59:59], "Etc/UTC")

  def filter_by_date(query, opts \\ [field: :date])

  def filter_by_date(query = %Ecto.Query{}, opts)
      when opts in [[field: :date], [field: :birth_date]],
      do: query

  def filter_by_date(query = %Ecto.Query{}, start: d = %Date{}, field: field)
      when field in [:date, :birth_date] do
    d = if field == :date, do: to_utc_datetime!(d, :start), else: d

    w =
      case field do
        :date ->
          if has_named_binding?(query, :o),
            do: dynamic([o: o], field(o, ^field) >= ^d),
            else: dynamic([s], field(s, ^field) >= ^d)

        :birth_date ->
          dynamic([x: x], field(x, ^field) >= ^d)
      end

    where(query, ^w)
  end

  def filter_by_date(query = %Ecto.Query{}, end: d = %Date{}, field: field)
      when field in [:date, :birth_date] do
    d = if field == :date, do: to_utc_datetime!(d, :end), else: d

    w =
      case field do
        :date ->
          if has_named_binding?(query, :o),
            do: dynamic([o: o], field(o, ^field) <= ^d),
            else: dynamic([s], field(s, ^field) <= ^d)

        :birth_date ->
          dynamic([x: x], field(x, ^field) <= ^d)
      end

    where(query, ^w)
  end

  def filter_by_date(query = %Ecto.Query{}, year: y, month: m, field: field),
    do: filter_by_date(query, ym_to_dates(y, m) ++ [field: field])

  def filter_by_date(query = %Ecto.Query{}, year: y, field: field),
    do: filter_by_date(query, ym_to_dates(y) ++ [field: field])

  def filter_by_date(query = %Ecto.Query{}, start: s = %Date{}, end: e = %Date{}, field: field)
      when field in [:date, :birth_date] do
    query |> filter_by_date(start: s, field: field) |> filter_by_date(end: e, field: field)
  end

  def ym_to_dates(year) when is_integer(year) do
    [start: %Date{year: year, month: 1, day: 1}, end: %Date{year: year, month: 12, day: 31}]
  end

  def ym_to_dates(year, month) when is_integer(year) and month in 1..12 do
    s = %Date{year: year, month: month, day: 1}
    [start: s, end: Date.end_of_month(s)]
  end

  def timespan_number_of_months({y_mn, m_mn} = _ym_mn, {y_mx, m_mx} = _ym_mx)
      when is_integer(y_mn) and is_integer(y_mx) and y_mx >= y_mn and
             m_mn in 1..12 and m_mx in 1..12 do
    (y_mx - y_mn) * 12 + m_mx - m_mn + 1
  end

  def timespan_earliest_latest({y_mn, m_mn} = _ym_mn, {y_mx, m_mx} = _ym_mx)
      when is_integer(y_mn) and is_integer(y_mx) and y_mx >= y_mn and
             m_mn in 1..12 and m_mx in 1..12 do
    [earliest, latest] =
      from(o in Order, select: [min(o.date), max(o.date)])
      |> Repo.one()
      |> Enum.map(&DateTime.to_date/1)

    mn =
      if Date.before?(earliest, %Date{year: y_mn, month: m_mn, day: 1}),
        do: {y_mn, m_mn},
        else: {earliest.year, earliest.month}

    mx =
      if Date.after?(%Date{year: y_mx, month: m_mx, day: 1}, latest),
        do: {latest.year, latest.month},
        else: {y_mx, m_mx}

    {mn, mx}
  end

  def timespan_ym_to_opts_list({y_mn, m_mn} = _ym_mn, {y_mx, m_mx} = _ym_mx)
      when is_integer(y_mn) and is_integer(y_mx) and y_mx >= y_mn and
             m_mn in 1..12 and m_mx in 1..12 do
    {{y_early, m_early}, {y_late, m_late}} = timespan_earliest_latest({y_mn, m_mn}, {y_mx, m_mx})
    n_months = timespan_number_of_months({y_mn, m_mn}, {y_mx, m_mx})

    Enum.reduce_while(1..n_months, [ym_to_dates(y_early, m_early)], fn _, acc ->
      prev = hd(acc)
      next = Date.add(prev[:end], 1)

      if {next.year, next.month} <= {y_late, m_late},
        do: {:cont, [ym_to_dates(next.year, next.month) | acc]},
        else: {:halt, acc}
    end)
    |> Enum.reverse()
  end

  def by_employee_by_product(eid, pid, opts) when is_list(opts),
    do: by_employee_by_product(eid, pid) |> filter_by_date(opts)

  def by_employee_by_product(eid, pid) do
    from(p in Product, as: :p)
    |> join(:inner, [p: p], od in assoc(p, :order_details), as: :od)
    |> join(:inner, [od: od], o in assoc(od, :order), as: :o)
    |> join(:inner, [o: o], e in assoc(o, :employee), as: :e)
    |> where([p: p, e: e], p.id == ^pid and e.id == ^eid)
    |> select([p: p, e: e, od: od], %{
      product: p.name,
      employee: e.last_name,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
  end

  def by_employee_by_product_all do
    for eid <- Repo.all(from(e in Employee, select: e.id)) do
      for pid <- Repo.all(from(p in Product, select: p.id)) do
        Repo.one(by_employee_by_product(eid, pid))
      end
    end
    |> List.flatten()
    |> Enum.reject(&is_nil(Map.values(&1) |> Enum.uniq() |> hd))
  end

  def benchmark(query = %Ecto.Query{}, kind \\ :all, reps \\ 1_000)
      when kind in [:all, :one] and is_integer(reps) do
    rf =
      case kind do
        :all -> fn x -> Repo.all(x) end
        :one -> fn x -> Repo.one(x) end
      end

    1..reps
    |> Enum.map(fn _ -> :timer.tc(fn -> rf.(query) end) end)
    |> Enum.sum_by(&elem(&1, 0))
    |> Kernel./(reps)
  end

  def query_entity_window_dynamic(m, xm, opts \\ [])
      when (m in @lhs or m in @rhs) and is_atom(xm) and is_list(opts) do
    agg = Keyword.get(opts, :agg, :sum)
    metric = Keyword.get(opts, :metric, :revenue)
    partition_by = Keyword.get(opts, :partition_by, :id)
    order = Keyword.get(opts, :order, :desc)
    limit = Keyword.get(opts, :limit)

    q = Joins.xy(m, Product) |> distinct(true)

    d_xm = dynamic([x: x], field(x, ^xm))
    d_pb = dynamic([x: x], field(x, ^partition_by))
    d_agg = dynamic_agg(agg, metric)

    q =
      from(q,
        select: ^%{x: d_xm, agg: d_agg},
        windows: [part: [partition_by: ^d_pb]],
        order_by: ^[{order, d_agg}]
      )

    if is_integer(limit) and limit > 0, do: limit(q, ^limit), else: q
  end

  def dynamic_agg(agg, :revenue) when agg in [:sum, :min, :max, :avg] do
    case agg do
      :sum -> dynamic([od: od, p: p], sum(od.quantity * p.price) |> over(:part))
      :min -> dynamic([od: od, p: p], min(od.quantity * p.price) |> over(:part))
      :avg -> dynamic([od: od, p: p], avg(od.quantity * p.price) |> over(:part))
      :max -> dynamic([od: od, p: p], max(od.quantity * p.price) |> over(:part))
    end
  end

  def dynamic_agg(agg, :quantity) when agg in [:sum, :min, :max, :avg] do
    case agg do
      :sum -> dynamic([od: od], sum(od.quantity) |> over(:part))
      :min -> dynamic([od: od], min(od.quantity) |> over(:part))
      :avg -> dynamic([od: od], avg(od.quantity) |> over(:part))
      :max -> dynamic([od: od], max(od.quantity) |> over(:part))
    end
  end

  def dynamic_agg(agg, metric, :rolling, field \\ :agg)
      when agg in [:sum, :min, :max, :avg] and metric in [:revenue, :quantity] and
             is_atom(field) do
    d_field = dynamic([s], field(s, ^field))

    case agg do
      :sum ->
        dynamic([s], sum(^d_field) |> over(:part))

      :min ->
        dynamic([s], min(^d_field) |> over(:part))

      :avg ->
        dynamic([s], avg(^d_field) |> over(:part))

      :max ->
        dynamic([s], max(^d_field) |> over(:part))
    end
  end

  def revenues_running_total(opts) when is_list(opts),
    do: revenues_running_total() |> filter_by_date(opts)

  def revenues_running_total() do
    Joins.xy(Order, Product)
    |> order_by([o: o], asc: o.date)
    |> windows([od: od], w: [order_by: [asc: od.id]])
    |> select([o: o, od: od, p: p], %{
      date: o.date,
      order_id: o.id,
      od_id: od.id,
      agg: sum(od.quantity * p.price) |> over(:w)
    })
  end

  def revenues_running_total_per_customer() do
    Joins.xy(Customer, Product)
    |> windows([x: x, o: o], part: [partition_by: x.id, order_by: [asc: o.date]])
    |> select([x: x, o: o, od: od, p: p], %{
      x_id: x.id,
      x: x.name,
      date: o.date,
      order_id: o.id,
      agg: sum(od.quantity * p.price) |> over(:part)
    })
    |> distinct(true)
  end

  def running(m, opts \\ []) when (m in @lhs or m in @rhs or m == Product) and is_list(opts) do
    agg = Keyword.get(opts, :agg, :sum)
    metric = Keyword.get(opts, :metric, :revenue)
    order = Keyword.get(opts, :order, :asc)
    date_opts = fetch_date_filter_opts(opts)

    Joins.xy(m, Order)
    |> window_expanding_by_order_date(order: order)
    |> Joins.merge_id()
    |> Joins.merge_order_id()
    |> Joins.merge_name()
    |> Joins.merge_agg(agg, metric)
    |> distinct(true)
    |> filter_by_date(date_opts)
  end

  def fetch_date_filter_opts(opts \\ []) when is_list(opts) do
    Enum.reduce([:year, :month, :start, :end], [], fn k, acc ->
      v = Keyword.get(opts, k)
      if !is_nil(v), do: Keyword.put(acc, k, v), else: acc
    end)
    |> Keyword.put(:field, :date)
    |> Enum.reverse()
  end

  def disagg_rows_by_field(rows, field \\ :name) when is_list(rows) and is_atom(field) do
    Enum.reduce(rows, %{}, fn r, acc ->
      Map.update(acc, r[field], [Map.delete(r, field)], &[Map.delete(r, field) | &1])
    end)
    |> Enum.map(fn {field_key, disagg_rows} -> {field_key, Enum.reverse(disagg_rows)} end)
    |> Map.new()
  end

  def calculate_sum_of_max_running_totals(disaggregated) when is_map(disaggregated) do
    disaggregated
    |> Enum.map(fn {_k, rows} -> Enum.sort_by(rows, & &1.agg, :desc) |> hd end)
    |> Enum.sum_by(& &1.agg)
  end

  def window_expanding_by_order_date(
        %Ecto.Query{from: %{source: {_table, m}}} = query,
        opts \\ []
      )
      when m in @m_tables and is_list(opts) do
    order = Keyword.get(opts, :order, :asc)

    case m do
      Product ->
        windows(query, [p: p, o: o],
          part: [partition_by: p.id, order_by: [{^order, field(o, :date)}]]
        )

      _ ->
        windows(query, [x: x, o: o],
          part: [partition_by: x.id, order_by: [{^order, field(o, :date)}]]
        )
    end
  end

  def window_sliding_by_order_date(%Ecto.SubQuery{} = query, n \\ 7, opts \\ [])
      when is_integer(n) and n > 0 and is_list(opts) do
    partition = Keyword.get(opts, :partition)
    d_frag = dynamic(fragment("ROWS ? PRECEDING", ^n - 1))

    if is_nil(partition) do
      windows(query, [s], part: [order_by: [asc: s.date], frame: ^d_frag])
    else
      case partition do
        {:x, field} ->
          d_part = dynamic([s], field(s, ^field))
          d_order = [{:asc, dynamic([s], field(s, :date))}]

          windows(query, [s], part: [partition_by: ^d_part, order_by: ^d_order, frame: ^d_frag])
      end
    end
  end

  def query_order_revenues(xm, ym), do: query_order_metric(xm, ym, :revenue)
  def query_order_revenues(), do: query_order_metric(Product, Order, :revenue)

  def query_order_metric(xm, ym, metric, opts \\ [])
      when xm != ym and (xm in @lhs or xm in @rhs or xm in [Product, Order]) and
             (ym in @lhs or ym in @rhs or ym in [Product, Order]) and
             metric in [:revenue, :quantity] and is_list(opts) do
    q =
      Joins.xy(xm, ym)
      |> group_by([o: o], o.id)
      |> Joins.merge_xy_ids(ym)
      |> Joins.merge_order_id()
      |> Joins.merge_order_date()
      |> Joins.merge_metric(metric)

    name = Keyword.get(opts, :name)

    if xm == Customer and name == :country,
      do: Joins.merge_name(q, xm, name),
      else: Joins.merge_name(q)
  end

  def rolling_avg_of_order_revenues(n \\ 7) when is_integer(n) and n > 0,
    do: rolling_agg_of_order_revenues(:avg, n)

  def rolling_agg_of_order_revenues(agg, n \\ 7)
      when is_integer(n) and n > 0 and agg in [:avg, :min, :max, :sum],
      do: rolling(n, agg: agg, metric: :revenue)

  def rolling(n, opts \\ [])
      when is_integer(n) and n > 0 and is_list(opts) do
    agg = Keyword.get(opts, :agg, :sum)
    metric = Keyword.get(opts, :metric, :revenue)
    date_opts = fetch_date_filter_opts(opts)
    partition = Keyword.get(opts, :partition)

    query_order_metric(Product, Order, metric)
    |> subquery()
    |> window_sliding_by_order_date(n, partition: partition)
    |> select([s], %{date: s.date})
    |> Joins.merge_from_subquery()
    |> Joins.merge_agg(agg, metric, :agg)
    |> filter_by_date(date_opts)
  end

  def plot(data, opts \\ []) when is_list(data) and is_list(opts) do
    scale = Keyword.get(opts, :scale, 80)
    symbol = Keyword.get(opts, :symbol, "#")

    data
    |> Enum.each(fn %{date: x, agg: y} ->
      xp = x |> DateTime.to_date() |> Date.to_string()
      y_mx = Enum.max_by(data, & &1.agg) |> Map.get(:agg)
      y_val = if is_float(y), do: Float.round(y, 2), else: y

      yp =
        (y / y_mx * scale)
        |> round()
        |> then(&Enum.reduce(1..&1, "", fn _, acc -> acc <> symbol end))
        |> Kernel.<>(" (#{y_val})")

      IO.puts("#{xp}\t#{yp}")
    end)
  end
end
