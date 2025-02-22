defmodule NorthwindElixirTraders.Category do
  use Ecto.Schema
  import Ecto.Changeset

  @name_mxlen 50
  @desc_mxlen 100

  schema "categories" do
    field(:name, :string)
    field(:description, :string)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:name, :description]
    required = [:name]

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> validate_length(:description, max: @desc_mxlen)
    |> unique_constraint([:name])
  end
end
