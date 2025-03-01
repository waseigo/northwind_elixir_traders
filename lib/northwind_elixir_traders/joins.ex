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
    Customer
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

  def to_p_od_and_group(m), do: entity_to_p_od(m) |> group_by([x: x], x.id)

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
end
