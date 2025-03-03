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
    Insights
  }

  @tables [Supplier, Category, Product, OrderDetail, Order, Employee, Shipper, Customer]
  @lhs Enum.slice(@tables, 0..1)
  @rhs Enum.slice(@tables, -3..-1)

  def get_tables(:lhs), do: @lhs
  def get_tables(:rhs), do: @rhs
  def get_tables(:both), do: @lhs ++ @rhs
  def get_tables(:all), do: @tables

  def base_from(m) when m in @lhs or m in @rhs or m == Product, do: from(x in m, as: :x)

  def entity_to_p_od(m) when m == Product do
    base_from(m) |> join(:inner, [x: x], od in assoc(x, :order_details), as: :od)
  end

  def entity_to_p_od(m) when m in @lhs do
    base_from(m)
    |> join(:inner, [x: x], p in assoc(x, :products), as: :p)
    |> join(:inner, [p: p], od in assoc(p, :order_details), as: :od)
  end

  def entity_to_p_od(m) when m in @rhs do
    base_from(m)
    |> join(:inner, [x: x], o in assoc(x, :orders), as: :o)
    |> join(:inner, [o: o], od in assoc(o, :order_details), as: :od)
    |> join(:inner, [od: od], p in assoc(od, :product), as: :p)
  end

  def to_p_od_and_group(m), do: to_p_od_and_group(m, :id)

  def to_p_od_and_group(m, field) when is_atom(field) do
    d_field = dynamic([x: x], field(x, ^field))
    entity_to_p_od(m) |> group_by(^d_field)
  end

  def p_od_group_and_select(m, field, opts)
      when is_list(opts) and m == Customer and field == :country,
      do: p_od_group_and_select(m, field) |> Insights.filter_by_date(opts)

  def p_od_group_and_select(m, field) when m == Customer and field == :country do
    to_p_od_and_group(m, field)
    |> select([x: x, od: od, p: p], %{
      id: x.id,
      country: x.country,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
  end

  def p_od_group_and_select(m, opts) when is_list(opts),
    do: p_od_group_and_select(m) |> Insights.filter_by_date(opts)

  def p_od_group_and_select(m) when m == Product do
    to_p_od_and_group(m)
    |> select([x: x, od: od], %{
      id: x.id,
      name: x.name,
      quantity: sum(od.quantity),
      revenue: sum(x.price * od.quantity)
    })
  end

  def p_od_group_and_select(m) when m in @lhs do
    to_p_od_and_group(m)
    |> select([x: x, p: p, od: od], %{
      id: x.id,
      name: x.name,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
  end

  def p_od_group_and_select(m) when m in @rhs do
    to_p_od_and_group(m)
    |> select([x: x, od: od, p: p], %{
      id: x.id,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
    |> rhs_merge_name(m)
  end

  def rhs_merge_name(%Ecto.Query{} = query, m) when m == Employee,
    do:
      select_merge(query, [x: x], %{
        name: fragment("? || ' ' || ?", x.last_name, x.first_name)
      })

  def rhs_merge_name(%Ecto.Query{} = query, m) when m in @rhs,
    do: select_merge(query, [x: x], %{name: x.name})

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
  def xy(xm, Order) when xm in @lhs, do: xm |> join_lhs_to_od() |> join_od_to_order()
  def xy(xm, Product) when xm in @rhs, do: xm |> join_rhs_to_od() |> join_od_to_product()

  # Connect Product to LHS schemas, and Order to RHS schemas
  def xy(Product, ym) when ym in @lhs, do: xy(Product, Order) |> join_lhs_to_product(ym)
  def xy(Order, ym) when ym in @rhs, do: xy(Order, Product) |> join_rhs_to_order(ym)

  # Connect LHS schemas to Product, and RHS schemas to Order
  def xy(xm, Product) when xm in @lhs, do: xm |> join_lhs_to_od() |> join_od_to_order()
  def xy(xm, Order) when xm in @rhs, do: xm |> join_rhs_to_od() |> join_od_to_product()

  # Connect Product to RHS schemas, and Order to LHS schemas
  def xy(Product, ym) when ym in @rhs, do: xy(Product, Order) |> join_rhs_to_order(ym)
  def xy(Order, ym) when ym in @lhs, do: xy(Order, Product) |> join_lhs_to_product(ym)

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
end
