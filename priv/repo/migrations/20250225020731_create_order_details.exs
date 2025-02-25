defmodule NorthwindElixirTraders.Repo.Migrations.CreateOrderDetails do
  use Ecto.Migration

  def change do
    create table(:order_details) do
      add :quantity, :integer
      add :order_id, references(:orders)
      add :product_id, references(:products)

      timestamps(type: :utc_datetime)
    end
  end
end
