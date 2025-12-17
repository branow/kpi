Class Lab5.Customer Extends Lab5.Person
{

Property DriverLicenseNumber As %String(MAXLEN = 15, MINLEN = 5, POPSPEC = "##class(Lab5.PopulateHelper).GetDriverLicense()") [ Required ];

Index DlnIndex On DriverLicenseNumber [ Unique ];

Relationship RentalHistory As Lab5.RentalContract [ Cardinality = many, Inverse = Customer ];

Property LicenseScan As %Stream.GlobalBinary;

Method AddRental(contract As Lab5.RentalContract)
{
    Do ..RentalHistory.Insert(contract)
}

Method RequestRental(car As Lab5.Car, startDate As %Date, endDate As %Date) As Lab5.RentalContract
{
    Set contract = ##class(Lab5.RentalContract).%New()
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
<Value>DriverLicenseNumber</Value>
</Value>
<Value name="2">
<Value>LicenseScan</Value>
</Value>
</Data>
<DefaultData>CustomerDefaultData</DefaultData>
<Type>%Storage.Persistent</Type>
}

}
