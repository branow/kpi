Class Lab4.Car Extends %Persistent
{

Property Vin As %String [ Required ];

Index VinIndex On Vin [ Unique ];

Property LicensePlate As %String;

Index LicensePlateIndex On LicensePlate [ Unique ];

Property Status As %String;

Property Mileage As %Integer(MAXVAL = 500000, MINVAL = 0);

Property DailyRate As %Decimal(MAXVAL = 500.0, MINVAL = 10.0);

Property Photo As %Stream.GlobalBinary;

Property Model As Lab4.CarModel;

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
<Value>vin</Value>
</Value>
<Value name="3">
<Value>licensePlate</Value>
</Value>
<Value name="4">
<Value>status</Value>
</Value>
<Value name="5">
<Value>mileage</Value>
</Value>
<Value name="6">
<Value>dailyRate</Value>
</Value>
<Value name="7">
<Value>photo</Value>
</Value>
<Value name="8">
<Value>model</Value>
</Value>
<Value name="9">
<Value>Vin</Value>
</Value>
<Value name="10">
<Value>LicensePlate</Value>
</Value>
<Value name="11">
<Value>Status</Value>
</Value>
<Value name="12">
<Value>Mileage</Value>
</Value>
<Value name="13">
<Value>DailyRate</Value>
</Value>
<Value name="14">
<Value>Photo</Value>
</Value>
<Value name="15">
<Value>Model</Value>
</Value>
</Data>
<DataLocation>^Lab4.CarD</DataLocation>
<DefaultData>CarDefaultData</DefaultData>
<IdLocation>^Lab4.CarD</IdLocation>
<IndexLocation>^Lab4.CarI</IndexLocation>
<StreamLocation>^Lab4.CarS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
