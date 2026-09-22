[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "wind_power_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared wind_power = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "cd28528f-6aba-4d1e-b9ed-f6fd6c34a914"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "df0ea3b0-e05d-46e1-b100-a00fd84c1351"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "wind_power", ItemKind = "Table"]}[Data],
  #"Rounded off" = Table.TransformColumns(#"Navigation 2", {{"wind_speed", each Number.Round(_, 2), type number}, {"energy_produced", each Number.Round(_, 2), type number}}),
  #"Inserted day" = Table.AddColumn(#"Rounded off", "Day", each Date.Day([date]), type nullable number),
  #"Inserted month" = Table.AddColumn(#"Inserted day", "Month", each Date.Month([date]), type nullable number),
  #"Inserted quarter" = Table.AddColumn(#"Inserted month", "Quarter", each Date.QuarterOfYear([date]), type nullable number),
  #"Inserted year" = Table.AddColumn(#"Inserted quarter", "Year", each Date.Year([date]), type nullable number),
  #"Changed column type" = Table.TransformColumnTypes(#"Inserted year", {{"Day", Int64.Type}, {"Month", Int64.Type}, {"Quarter", Int64.Type}, {"Year", Int64.Type}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Day", "day"}, {"Month", "month"}, {"Quarter", "quarter"}, {"Year", "year"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns", "-", ":", Replacer.ReplaceText, {"time"}),
  #"Duplicated column" = Table.DuplicateColumn(#"Replaced value", "time", "time - Copy"),
  #"Split column by delimiter" = Table.SplitColumn(#"Duplicated column", "time - Copy", Splitter.SplitTextByDelimiter(":"), {"time - Copy.1", "time - Copy.2", "time - Copy.3"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Split column by delimiter", {{"time - Copy.1", Int64.Type}, {"time - Copy.2", Int64.Type}, {"time - Copy.3", Int64.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 1", {{"time - Copy.1", "hour_of_day"}, {"time - Copy.2", "minute_of_hour"}, {"time - Copy.3", "second_of_minute"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 1", "time_period", each if [hour_of_day]>=5 and [hour_of_day]< 12 then "Morning"
else if [hour_of_day]>= 12 and [hour_of_day]< 17 then "Afternoon"
else if [hour_of_day]>= 17 and [hour_of_day]< 21 then "Evening"
else "Night"), {{"time_period", type text}})
in
  #"Added custom";
shared wind_power_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "cd28528f-6aba-4d1e-b9ed-f6fd6c34a914"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2c72e86b-4ccb-4acc-a983-63f0e9bd49e7"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "wind_power", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
