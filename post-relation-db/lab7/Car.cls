Class Lab7.Car Extends (%Persistent, %Library.Populate, %JSON.Adaptor, %XML.Adaptor)
{

Property Vin As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetVIN()") [ Required ];

Index VinIndex On Vin [ Unique ];

Property LicensePlate As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetLicensePlate()");

Index LicensePlateIndex On LicensePlate [ Unique ];

Property Status As %String(POPSPEC = "##class(Lab7.PopulateHelper).GetCarStatus()");

Property Mileage As %Integer(MAXVAL = 500000, MINVAL = 0);

Property DailyRate As %Decimal(MAXVAL = 500.0, MINVAL = 10.0);

Property Photo As %Stream.GlobalBinary;

Property Model As Lab7.CarModel;

Query FindCarsByMaxRate(MaxRate As %Decimal) As %SQLQuery(CONTAINID = 1, ROWSPEC = "Id:%Integer,Vin:%String,LicensePlate:%String,Brand:%String,ModelName:%String,DailyRate:%Decimal") [ SqlProc ]
{
    SELECT %ID, Vin, LicensePlate, Model->Brand, Model->ModelName, DailyRate
    FROM Lab7.Car
    WHERE DailyRate <= :MaxRate
    ORDER BY DailyRate ASC
}

Trigger ValidateMileageUpdate [ Event = UPDATE, Foreach = row/object ]
{
    If {Mileage*C} {
        If ({Mileage*O} > 0) && ({Mileage} < {Mileage*O}) {
            Set %ok = 0
            Set %msg = "ПОМИЛКА: Скручування пробігу заборонено! (" _ {Mileage*O} _ " -> " _ {Mileage} _ ")"
        }
    }
}

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
<DataLocation>^Lab7.CarD</DataLocation>
<DefaultData>CarDefaultData</DefaultData>
<IdLocation>^Lab7.CarD</IdLocation>
<IndexLocation>^Lab7.CarI</IndexLocation>
<StreamLocation>^Lab7.CarS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
