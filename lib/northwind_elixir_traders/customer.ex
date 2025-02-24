defmodule NorthwindElixirTraders.Customer do
  use Ecto.Schema
  import Ecto.Changeset
  # alias NorthwindElixirTraders.Order

  @name_mxlen 50

  schema "customers" do
    field(:name, :string)
    field(:contact_name, :string)
    field(:address, :string)
    field(:city, :string)
    field(:postal_code, :string)
    field(:country, :string)
    # has_many(:orders, Order)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:id, :name, :contact_name, :address, :city, :postal_code, :country]
    required = permitted |> List.delete(:id)

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> unique_constraint([:name])
    |> unique_constraint([:id])
  end
end
