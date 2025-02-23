defmodule NorthwindElixirTraders.DataImporter do
  require Logger
  alias NorthwindElixirTraders.Repo

  @name :nt
  @database "NorthwindTraders-original.db"

  def start() do
    if is_nil(Process.whereis(@name)),
      do: Repo.start_link(name: @name, database: @database)
  end

  def switch(name) when name in [@name, Repo] do
    try do
      if name == @name, do: start()
      Repo.put_dynamic_repo(name)
      Logger.debug("Switched to #{name}")
      {:ok, Repo.get_dynamic_repo()}
    catch
      _ ->
        Logger.debug("Error: could not switch to #{name}")
        {:error, Repo.get_dynamic_repo()}
    end
  end

  def switch() do
    case Repo.get_dynamic_repo() do
      @name -> switch(Repo)
      _ -> switch(@name)
    end
  end

  def nt_query(sql) when is_bitstring(sql) do
    switch(:nt)
    result = Repo.query(sql)
    switch()
    result
  end

  def singularize(plural) when is_bitstring(plural) do
    ending = String.slice(plural, -3..-1)

    if ending == "ies" do
      String.replace(plural, ending, "y")
    else
      String.trim(plural, "s")
    end
  end

  def pluralize(singular) when is_bitstring(singular) do
    last_char = String.last(singular)

    case last_char do
      "y" -> String.trim(singular, last_char) <> "ies"
      _ -> singular <> "s"
    end
  end
end
