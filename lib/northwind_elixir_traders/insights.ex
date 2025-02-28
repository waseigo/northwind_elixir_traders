defmodule NorthwindElixirTraders.Insights do
  import Ecto.Query
  alias NorthwindElixirTraders.{Repo, Product, Order, OrderDetail, Customer}
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

  def query_customers_by_order_revenue do
    from(c in Customer,
      join: o in assoc(c, :orders),
      join: od in assoc(o, :order_details),
      join: p in assoc(od, :product),
      group_by: c.id,
      select: %{id: c.id, name: c.name, revenue: sum(od.quantity * p.price)}
    )
  end

  def query_top_n_customers_by_order_revenue(n \\ 5) do
    from(s in subquery(query_customers_by_order_revenue()),
      order_by: [desc: s.revenue],
      limit: ^n
    )
  end

  def calculate_top_n_customers_by_order_value(n \\ 5)
      when is_integer(n) and n >= 0 do
    if n == 0,
      do: 0,
      else:
        from(s in subquery(query_top_n_customers_by_order_revenue(n)),
          select: sum(s.revenue)
        )
        |> Repo.one()
  end

  def count_customers_with_revenues do
    from(s in subquery(query_customers_by_order_revenue()),
      where: s.revenue > 0,
      select: count(s.id)
    )
    |> Repo.one()
  end

  def count_customers_orders(condition \\ :with)
      when condition in [:with, :without] do
    count_with =
      from(c in Customer)
      |> join(:inner, [c], o in assoc(c, :orders))
      |> select([c], c.id)
      |> distinct(true)
      |> Repo.aggregate(:count)

    case condition do
      :with -> count_with
      :without -> Repo.aggregate(Customer, :count) - count_with
    end
  end

  def generate_customer_share_of_revenues_xy do
    nc = count_customers_orders(:with)
    total = Order |> Repo.all() |> calculate_total_value_of_orders()

    Task.async_stream(
      0..nc,
      &{&1 / nc, calculate_top_n_customers_by_order_value(&1) / total}
    )
    |> Enum.to_list()
    |> Enum.map(&elem(&1, 1))
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
end
