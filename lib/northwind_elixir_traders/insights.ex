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

  @tables [Customer, Employee, Shipper, Category, Supplier, Product, OrderDetail, Order]
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

  def revenue_share_total_trivial_many(m, q \\ 0.8) do
    calculate_relative_revenue_share_of_entity_rows(m)
    |> Enum.reverse()
    |> helper_vital_trivial(m, q)
  end

  def revenue_share_total_vital_few(m, q \\ 0.2) do
    calculate_relative_revenue_share_of_entity_rows(m) |> helper_vital_trivial(m, q)
  end

  def helper_vital_trivial(data, m, q)
      when is_list(data) and m in @m_tables and is_number(q) and q > 0 and q <= 1 do
    n = m |> count_entity_orders() |> Kernel.*(q) |> round()
    data |> Enum.take(n) |> Enum.sum_by(& &1.share)
  end
end
