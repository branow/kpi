Class Lab5.Car Extends (%Persistent, %Library.Populate)
{

Property Vin As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetVIN()") [ Required ];

Index VinIndex On Vin [ Unique ];

Property LicensePlate As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetLicensePlate()");

Index LicensePlateIndex On LicensePlate [ Unique ];

Property Status As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetCarStatus()");

Property Mileage As %Integer(MAXVAL = 500000, MINVAL = 0);

Property DailyRate As %Decimal(MAXVAL = 500.0, MINVAL = 10.0);

Property Photo As %Stream.GlobalBinary;

Property Model As Lab5.CarModel;

Method IsAvailable(startDate As %Date, endDate As %Date) As %Boolean
{
    Quit (..Status = "Available")
}

Storage Default
{
<Data name="CarDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>Vin</Value>
</Value>
<Value name="3">
<Value>LicensePlate</Value>
</Value>
<Value name="4">
<Value>Status</Value>
</Value>
<Value name="5">
<Value>Mileage</Value>
</Value>
<Value name="6">
<Value>DailyRate</Value>
</Value>
<Value name="7">
<Value>Photo</Value>
</Value>
<Value name="8">
<Value>Model</Value>
</Value>
</Data>
<DataLocation>^Lab5.CarD</DataLocation>
<DefaultData>CarDefaultData</DefaultData>
<IdLocation>^Lab5.CarD</IdLocation>
<IndexLocation>^Lab5.CarI</IndexLocation>
<StreamLocation>^Lab5.CarS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
