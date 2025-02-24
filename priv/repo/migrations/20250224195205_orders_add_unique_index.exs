defmodule NorthwindElixirTraders.Repo.Migrations.OrdersAddUniqueIndex do
  use Ecto.Migration

  def change do
    create unique_index(:orders, [:customer_id, :employee_id, :shipper_id, :date])
  end
end
