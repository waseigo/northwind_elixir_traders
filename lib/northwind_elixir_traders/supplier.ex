defmodule NorthwindElixirTraders.Supplier do
  use Ecto.Schema
  import Ecto.Changeset

  @name_mxlen 50

  schema "suppliers" do
    field(:name, :string)
    field(:contact_name, :string)
    field(:address, :string)
    field(:city, :string)
    field(:postal_code, :string)
    field(:country, :string)
    field(:phone, :string)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:name, :contact_name, :address, :city, :postal_code, :country, :phone]
    required = [:name]

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> unique_constraint([:name])
  end
end
