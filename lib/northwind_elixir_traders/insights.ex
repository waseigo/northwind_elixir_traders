defmodule NorthwindElixirTraders.Insights do
  import Ecto.Query
  alias NorthwindElixirTraders.{Repo, Product, Order, OrderDetail}

  def query_order_detail_values(order_id) do
    OrderDetail
    |> join(:inner, [od], p in Product, on: od.product_id == p.id)
    |> where([od], od.order_id == ^order_id)
    |> select([od, p], od.quantity * p.price)
  end
end
