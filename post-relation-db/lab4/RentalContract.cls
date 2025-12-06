Class Lab4.RentalContract Extends %Persistent
{

Property ContractId As %Integer [ Required ];

Index ContractIdIndex On ContractId [ PrimaryKey ];

Property StartDate As %Date [ Required ];

Property EndDate As %Date [ Required ];

Property AgreementText As %Stream.GlobalCharacter;

Property Car As Lab4.Car;

Property Customer As Lab4.Customer;

Relationship Payments As Lab4.Payment [ Cardinality = children, Inverse = RentalContract ];

Property Closed As %Boolean;

Method AddPayment(p As Lab4.Payment)
{
    Do ..Payments.Insert(p)
}

Method CalculateTotal() As %Decimal
{
    Set total=0
    For i=1:1:..Payments.Count() {
        Set total = total + ..Payments.GetAt(i).amount
    }
    Quit total
}

Method CloseContract()
{
    Set ..Closed = 1
}

Method IsActive() As %Boolean
{
    Quit ('..Closed)
}

Storage Default
{
<Data name="RentalContractDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>contractId</Value>
</Value>
<Value name="3">
<Value>startDate</Value>
</Value>
<Value name="4">
<Value>endDate</Value>
</Value>
<Value name="5">
<Value>agreementText</Value>
</Value>
<Value name="6">
<Value>car</Value>
</Value>
<Value name="7">
<Value>customer</Value>
</Value>
<Value name="8">
<Value>payments</Value>
</Value>
<Value name="9">
<Value>closed</Value>
</Value>
<Value name="10">
<Value>ContractId</Value>
</Value>
<Value name="11">
<Value>StartDate</Value>
</Value>
<Value name="12">
<Value>EndDate</Value>
</Value>
<Value name="13">
<Value>AgreementText</Value>
</Value>
<Value name="14">
<Value>Car</Value>
</Value>
<Value name="15">
<Value>Customer</Value>
</Value>
<Value name="16">
<Value>Payments</Value>
</Value>
<Value name="17">
<Value>Closed</Value>
</Value>
</Data>
<DataLocation>^Lab4.RentalContractD</DataLocation>
<DefaultData>RentalContractDefaultData</DefaultData>
<IdLocation>^Lab4.RentalContractD</IdLocation>
<IndexLocation>^Lab4.RentalContractI</IndexLocation>
<StreamLocation>^Lab4.RentalContractS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
