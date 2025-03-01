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

  def entity_to_p_od(m) when m == Product do
    from(x in m)
    |> join(:inner, [x], od in assoc(x, :order_details))
  end

  def entity_to_p_od(m) when m in @lhs do
    from(x in m)
    |> join(:inner, [x], p in assoc(x, :products))
    |> join(:inner, [x, p], od in assoc(p, :order_details))
  end

  def entity_to_p_od(m) when m in @rhs do
    from(x in m)
    |> join(:inner, [x], o in assoc(x, :orders))
    |> join(:inner, [x, o], od in assoc(o, :order_details))
    |> join(:inner, [x, o, od], p in assoc(od, :product))
  end

  def to_p_od_and_group(m), do: entity_to_p_od(m) |> group_by([x], x.id)

  def p_od_group_and_select(m) when m == Product do
    to_p_od_and_group(m)
    |> select([x, od], %{
      id: x.id,
      name: x.name,
      quantity: sum(od.quantity),
      revenue: sum(x.price * od.quantity)
    })
  end

  def p_od_group_and_select(m) when m in @lhs do
    to_p_od_and_group(m)
    |> select([x, p, od], %{
      id: x.id,
      name: x.name,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
  end

  def p_od_group_and_select(m) when m in @rhs do
    to_p_od_and_group(m)
    |> select([x, o, od, p], %{
      id: x.id,
      quantity: sum(od.quantity),
      revenue: sum(p.price * od.quantity)
    })
  end
end
