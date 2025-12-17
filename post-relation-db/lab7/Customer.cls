Class Lab7.Customer Extends Lab7.Person
{

Property DriverLicenseNumber As %String(MAXLEN = 15, MINLEN = 5, POPSPEC = "##class(Lab7.PopulateHelper).GetDriverLicense()") [ Required ];

Index DlnIndex On DriverLicenseNumber [ Unique ];

Relationship RentalHistory As Lab7.RentalContract [ Cardinality = many, Inverse = Customer ];

Property LicenseScan As %Stream.GlobalBinary;

Method AddRental(contract As Lab7.RentalContract)
{
    Do ..RentalHistory.Insert(contract)
}

Method RequestRental(car As Lab7.Car, startDate As %Date, endDate As %Date) As Lab7.RentalContract
{
    Set contract = ##class(Lab7.RentalContract).%New()
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
