Class Lab4.Customer Extends Lab4.Person
{

Property DriverLicenseNumber As %String(MAXLEN = 15, MINLEN = 5) [ Required ];

Index DlnIndex On DriverLicenseNumber [ Unique ];

Property RentalHistory As list Of Lab4.RentalContract;

Property LicenseScan As %Stream.GlobalBinary;

Method AddRental(contract As Lab4.RentalContract)
{
    Do ..RentalHistory.Insert(contract)
}

Method RequestRental(car As Lab4.Car, startDate As %Date, endDate As %Date) As Lab4.RentalContract
{
    Set contract = ##class(Lab4.RentalContract).%New()
    Set contract.StartDate = startDate
    Set contract.EndDate = endDate
    Set contract.Car = car
    Set contract.Customer = ..%Id()
    Quit contract
}

Storage Default
{
<Data name="CustomerDefaultData">
<Subscript>"Customer"</Subscript>
<Value name="1">
<Value>driverLicenseNumber</Value>
</Value>
<Value name="2">
<Value>rentalHistory</Value>
</Value>
<Value name="3">
<Value>licenseScan</Value>
</Value>
<Value name="4">
<Value>DriverLicenseNumber</Value>
</Value>
<Value name="5">
<Value>RentalHistory</Value>
</Value>
<Value name="6">
<Value>LicenseScan</Value>
</Value>
</Data>
<DefaultData>CustomerDefaultData</DefaultData>
<Type>%Storage.Persistent</Type>
}

}
