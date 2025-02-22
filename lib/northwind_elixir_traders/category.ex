defmodule NorthwindElixirTraders.Category do
  use Ecto.Schema

  schema "categories" do
    field(:name, :string)
    field(:description, :string)
  end
end
