defmodule NorthwindElixirTraders.Validations do
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Repo, Country}

  def validate_foreign_key_id(changeset, target, field) when is_atom(field) do
    val = get_field(changeset, field)

    if is_nil(val) do
      add_error(changeset, field, "key '%{field}' not found in changeset",
        field: field,
        validation: :foreign_key_id
      )
    else
      target_record = Repo.get(target, val)

      case target_record do
        nil ->
          add_error(changeset, field, "no '%{record}' with primary key value '%{pkval}'",
            record: to_string(target),
            pkval: val,
            validation: :foreign_key_id
          )

        _ ->
          changeset
      end
    end
  end

  def validate_country(changeset, field) when is_atom(field) do
    case {changeset.errors[field], get_field(changeset, field)} do
      {nil, country} when not is_nil(country) ->
        if is_nil(Country.get_dial(country)) do
          add_error(
            changeset,
            field,
            "country '%{country}' not found in the Countries table",
            country: country,
            validation: :country
          )
        else
          changeset
        end

      _ ->
        changeset
    end
  end
end
