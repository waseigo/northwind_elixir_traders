defmodule NorthwindElixirTraders.Employee do
  use Ecto.Schema
  import Ecto.Changeset

  @name_mxlen 50
  @notes_mxlen 500

  schema "employees" do
    field(:last_name, :string)
    field(:first_name, :string)
    field(:birth_date, :date)
    field(:photo, :string)
    field(:notes, :string)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:last_name, :first_name, :birth_date, :photo, :notes]
    required = [:last_name, :first_name, :birth_date]

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:last_name, max: @name_mxlen)
    |> validate_length(:first_name, max: @name_mxlen)
    |> validate_length(:notes, max: @notes_mxlen)
    |> validate_age_range(:birth_date, min: 15, max: 100)
    |> unique_constraint([:last_name, :first_name, :birth_date])
  end

  defp validate_age_range(changeset, field, [min: age_mn, max: age_mx] = opts)
       when is_atom(field) and is_list(opts) do
    case {changeset.errors[field], get_field(changeset, field)} do
      {nil, field_value} when not is_nil(field_value) ->
        within_range? =
          field_value
          |> Date.diff(Date.utc_today())
          |> then(&Kernel.and(Kernel.>(&1, -age_mx * 365), Kernel.<(&1, -age_mn * 365)))

        if not within_range? do
          add_error(
            changeset,
            field,
            "date not within the specified span between '%{min}' and '%{max}' years ago",
            min: age_mn,
            max: age_mx,
            validation: :age_range
          )
        else
          changeset
        end

      _ ->
        changeset
    end
  end
end
