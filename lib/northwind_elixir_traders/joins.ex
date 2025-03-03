defmodule NorthwindElixirTraders.Joins do
  import Ecto.Query

  alias NorthwindElixirTraders.{
    Supplier,
    Category,
    Product,
    OrderDetail,
    Order,
    Employee,
    Shipper,
    Customer,
    Insights,
    Repo
  }

  @tables [Supplier, Category, Product, OrderDetail, Order, Employee, Shipper, Customer]
  @lhs Enum.slice(@tables, 0..1)
  @rhs Enum.slice(@tables, -3..-1)

  def get_tables(:lhs), do: @lhs
  def get_tables(:rhs), do: @rhs
  def get_tables(:both), do: @lhs ++ @rhs
  def get_tables(:all), do: @tables

  def base_from(m) when m in @lhs or m in @rhs, do: from(x in m, as: :x)
  def base_from(m) when m == Product, do: from(p in m, as: :p)
  def base_from(m) when m == Order, do: from(o in m, as: :o)

  def entity_to_p_od(m) when m == Product do
    base_from(m)
    |> join(:inner, [p: p], od in assoc(p, :order_details), as: :od)
    |> join(:inner, [od: od], o in assoc(od, :order), as: :o)
  end

  def entity_to_p_od(m) when m == Order do
    base_from(m)
    |> join(:inner, [o: o], od in assoc(o, :order_details), as: :od)
    |> join(:inner, [od: od], p in assoc(od, :product), as: :p)
    |> join(:inner, [o: o], c in assoc(o, :customer), as: :c)
  end

  def entity_to_p_od(m) when m in @lhs do
    base_from(m)
    |> join(:inner, [x: x], p in assoc(x, :products), as: :p)
    |> join(:inner, [p: p], od in assoc(p, :order_details), as: :od)
    |> join(:inner, [od: od], o in assoc(od, :order), as: :o)
  end

  def entity_to_p_od(m) when m in @rhs do
    base_from(m)
    |> join(:inner, [x: x], o in assoc(x, :orders), as: :o)
    |> join(:inner, [o: o], od in assoc(o, :order_details), as: :od)
    |> join(:inner, [od: od], p in assoc(od, :product), as: :p)
  end

  def to_p_od_and_group(m), do: to_p_od_and_group(m, :id)

  def to_p_od_and_group(m, field)
      when (m in @lhs or m in @rhs or m in [Product, Order]) and is_atom(field) do
    d_field =
      case m do
        Product -> dynamic([p: p], field(p, ^field))
        Order -> dynamic([o: o], field(o, ^field))
        _ -> dynamic([x: x], field(x, ^field))
      end

    entity_to_p_od(m) |> group_by(^d_field)
  end

  def p_od_group_and_select(m, field, opts)
      when is_list(opts) and m == Customer and field == :country,
      do: p_od_group_and_select(m, field) |> Insights.filter_by_date(opts)

  def p_od_group_and_select(m, field) when m == Customer and field == :country,
    do:
      to_p_od_and_group(m, field)
      |> select([x: x], %{id: x.id})
      |> merge_quantity_revenue()
      |> merge_name(field)

  def p_od_group_and_select(m, opts) when is_list(opts),
    do: p_od_group_and_select(m) |> Insights.filter_by_date(opts)

  def p_od_group_and_select(m) when m in @lhs or m in @rhs or m in [Product, Order] do
    q = to_p_od_and_group(m)

    case m do
      Product -> select(q, [p: p], %{id: p.id})
      Order -> select(q, [o: o], %{id: o.id})
      _ -> select(q, [x: x], %{id: x.id})
    end
    |> merge_quantity_revenue()
    |> merge_name()
  end

  def merge_quantity_revenue(%Ecto.Query{} = query),
    do:
      select_merge(query, [p: p, od: od], %{
        quantity: sum(od.quantity),
        revenue: sum(p.price * od.quantity)
      })

  def merge_name(%Ecto.Query{} = query, m, field) when m == Customer and field == :country,
    do: select_merge(query, [x: x], %{name: x.country})

  def merge_name(%Ecto.Query{} = query, m) when m == Order do
    d_frag =
      case Ecto.Adapter.lookup_meta(Repo)[:adapter] do
        Ecto.Adapters.SQLite3 ->
          dynamic([o: o, c: c], fragment("strftime('%Y-%m-%d', ?) || ' - ' || ?", o.date, c.name))

        Ecto.Adapters.Postgres ->
          dynamic(
            [o: o, c: c],
            fragment("to_char(?, 'YYYY-MM-DD') || ' - ' || ?", o.date, c.name)
          )

        _ ->
          dynamic([o: o, c: c], fragment("? || ' - ' || ?", o.date, c.name))
      end

    select_merge(query, [o: o, c: c], ^%{name: d_frag})
  end

  def merge_name(%Ecto.Query{} = query, m) when m == Employee,
    do:
      select_merge(query, [x: x], %{
        name: fragment("? || ' ' || ?", x.last_name, x.first_name)
      })

  def merge_name(%Ecto.Query{} = query, m) when m in @rhs or m in @lhs,
    do: select_merge(query, [x: x], %{name: x.name})

  def merge_name(%Ecto.Query{} = query, m) when m == Product,
    do: select_merge(query, [p: p], %{name: p.name})

  def merge_name(%Ecto.Query{from: %{source: {_table, m}}} = query, field) when is_atom(field),
    do: merge_name(query, m, field)

  def merge_name(%Ecto.Query{from: %{source: {_table, m}}} = query), do: merge_name(query, m)

  def module_to_assoc_field(m) when m in @tables do
    Module.split(m) |> List.last() |> String.downcase() |> String.to_atom()
  end

  def xy(xm, ym) when xm in @lhs and ym in @rhs,
    do: join_lhs_to_od(xm) |> join_od_to_order() |> join_rhs_to_order(ym)

  def xy(xm, ym) when xm in @rhs and ym in @lhs,
    do: join_rhs_to_od(xm) |> join_od_to_product() |> join_lhs_to_product(ym)

  # Connect Product and Order via OrderDetail (the order doesn't matter)
  def xy(Product, Order) do
    from(od in OrderDetail, as: :od)
    |> join_od_to_product()
    |> join_od_to_order()
  end

  def xy(Order, Product), do: xy(Product, Order)

  # Connect LHS to OrderDetail via Product, and RHS to OrderDetail via Order
  def xy(xm, Order) when xm in @lhs,
    do: xm |> join_lhs_to_od() |> join_od_to_order()

  def xy(xm, Product) when xm in @rhs,
    do: xm |> join_rhs_to_od() |> join_od_to_product()

  # Connect Product to LHS schemas, and Order to RHS schemas
  def xy(Product, ym) when ym in @lhs,
    do: xy(Product, Order) |> join_lhs_to_product(ym)

  def xy(Order, ym) when ym in @rhs,
    do: xy(Order, Product) |> join_rhs_to_order(ym)

  # Connect LHS schemas to Product, and RHS schemas to Order
  def xy(xm, Product) when xm in @lhs,
    do: xm |> join_lhs_to_od() |> join_od_to_order()

  def xy(xm, Order) when xm in @rhs,
    do: xm |> join_rhs_to_od() |> join_od_to_product()

  # Connect Product to RHS schemas, and Order to LHS schemas
  def xy(Product, ym) when ym in @rhs,
    do: xy(Product, Order) |> join_rhs_to_order(ym)

  def xy(Order, ym) when ym in @lhs,
    do: xy(Order, Product) |> join_lhs_to_product(ym)

  # Connect LHS and RHS, passing through OrderDetail
  def xy(xm, ym) when xm in @lhs and xm != ym,
    do: xm |> join_lhs_to_od() |> join_od_to_order() |> join_lhs_to_product(ym)

  def xy(xm, ym) when xm in @rhs and xm != ym,
    do: xm |> join_rhs_to_od() |> join_od_to_product() |> join_rhs_to_order(ym)

  def join_od_to_product(query = %Ecto.Query{}),
    do: join(query, :inner, [od: od], p in assoc(od, :product), as: :p)

  def join_od_to_order(query = %Ecto.Query{}),
    do: join(query, :inner, [od: od], o in assoc(od, :order), as: :o)

  def join_lhs_to_product(query = %Ecto.Query{}, ym) when ym in @lhs,
    do: join(query, :inner, [p: p], y in assoc(p, ^module_to_assoc_field(ym)), as: :y)

  def join_rhs_to_order(query = %Ecto.Query{}, ym) when ym in @rhs,
    do: join(query, :inner, [o: o], y in assoc(o, ^module_to_assoc_field(ym)), as: :y)

  def join_lhs_to_od(xm) when xm in @lhs do
    from(x in xm, as: :x)
    |> join(:inner, [x: x], p in assoc(x, :products), as: :p)
    |> join(:inner, [p: p], od in assoc(p, :order_details), as: :od)
  end

  def join_rhs_to_od(xm) when xm in @rhs do
    from(x in xm, as: :x)
    |> join(:inner, [x: x], o in assoc(x, :orders), as: :o)
    |> join(:inner, [o: o], od in assoc(o, :order_details), as: :od)
  end

  def gen_combinations() do
    for lhs <- @tables do
      for rhs <- @tables do
        if lhs != OrderDetail and rhs != OrderDetail and rhs != lhs do
          [{lhs, rhs}, {rhs, lhs}]
        end
      end
    end
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
  end

  def calc_total_revenues({xm, ym}),
    do: xy(xm, ym) |> select([p: p, od: od], sum(p.price * od.quantity)) |> Repo.one()

  def verify_total_revenues_of_xy(combs) do
    correct = 38_642_423
    combs |> Enum.map(&calc_total_revenues/1) |> Enum.reject(&(&1 != {correct, correct}))
  end
end
