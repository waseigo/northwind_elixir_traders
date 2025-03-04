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

  def filter_by_date(query = %Ecto.Query{}, start: s = %Date{}, field: field)
      when field in [:date, :birth_date] do
    s = if field == :date, do: to_utc_datetime!(s, :start), else: s

    w =
      case field do
        :date -> dynamic([o: o], field(o, ^field) >= ^s)
        :birth_date -> dynamic([x: x], field(x, ^field) >= ^s)
      end

    where(query, ^w)
  end

  def filter_by_date(query = %Ecto.Query{}, end: e = %Date{}, field: field)
      when field in [:date, :birth_date] do
    e = if field == :date, do: to_utc_datetime!(e, :end), else: e

    w =
      case field do
        :date -> dynamic([o: o], field(o, ^field) <= ^e)
        :birth_date -> dynamic([x: x], field(x, ^field) <= ^e)
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

  def dynamic_agg(agg, :revenue) when agg in [:sum, :min, :max, :avg, :count] do
    case agg do
      :sum -> dynamic([od: od, p: p], sum(od.quantity * p.price) |> over(:part))
      :min -> dynamic([od: od, p: p], min(od.quantity * p.price) |> over(:part))
      :avg -> dynamic([od: od, p: p], avg(od.quantity * p.price) |> over(:part))
      :max -> dynamic([od: od, p: p], max(od.quantity * p.price) |> over(:part))
      :count -> raise ArgumentError, ":count is not supported for the :revenue metric"
    end
  end

  def dynamic_agg(agg, :quantity) when agg in [:sum, :min, :max, :avg, :count] do
    case agg do
      :sum -> dynamic([od: od], sum(od.quantity) |> over(:part))
      :min -> dynamic([od: od], min(od.quantity) |> over(:part))
      :avg -> dynamic([od: od], avg(od.quantity) |> over(:part))
      :max -> dynamic([od: od], max(od.quantity) |> over(:part))
      :count -> dynamic([od: od], max(od.quantity) |> over(:part))
    end
  end

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
end
