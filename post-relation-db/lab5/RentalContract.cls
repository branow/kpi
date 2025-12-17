Class Lab5.RentalContract Extends (%Persistent, %Library.Populate)
{

Property StartDate As %Date(POPSPEC = "Date()") [ Required ];

Property EndDate As %Date(POPSPEC = "Date()") [ Required ];

Property AgreementText As %Stream.GlobalCharacter;

Property Car As Lab5.Car;

Relationship Customer As Lab5.Customer [ Cardinality = one, Inverse = RentalHistory ];

Relationship Payments As Lab5.Payment [ Cardinality = children, Inverse = RentalContract ];

Property Closed As %Boolean;

Property RentalDays As %Integer [ Calculated, SqlComputeCode = { Set {*RentalDays} = {EndDate} - {StartDate} + 1 }, SqlComputed ];

Property TotalPayment As %Decimal [ Calculated, SqlComputeCode = {
    Set {TotalPayment} = ##class(Lab5.RentalContract).%OpenId({%%ID}).TotalPaymentGet()
  }, SqlComputed ];

Method OnPopulate() As %Status
{
    Set duration = $Random(14) + 1
    Set ..EndDate = ..StartDate + duration

    Do ..AgreementText.Write(##class(Lab5.PopulateHelper).GetAgreementText())

    Do ..Payments.Clear()
    
    Set pCount = $Random(3) + 1
    For i=1:1:pCount {
        Set pay = ##class(Lab5.Payment).%New()
        
        Set pay.Amount = 50 + $Random(450)
        
        Set pay.PaymentDate = ..StartDate + $Random(duration)
        
        Set pay.Method = ##class(Lab5.PopulateHelper).GetPaymentMethod()
        Set pay.Processed = $Random(2)
        
        Do ..Payments.Insert(pay)
    }

    Quit $$$OK
}

Method RentalDaysGet() As %Integer
{
    If (..StartDate="") || (..EndDate="") Quit 0
    Quit (..EndDate - ..StartDate) + 1
}

Method TotalPaymentGet() As %Decimal
{
    Set total = 0
    Set count = ..Payments.Count()
    If count = 0 Quit 0
    
    For i=1:1:count {
        Set pay = ..Payments.GetAt(i)
        If $IsObject(pay) {
            Set total = total + pay.Amount
        }
    }
    Quit total
}

Method AddPayment(p As Lab5.Payment)
{
    Do ..Payments.Insert(p)
}

Method CloseContract()
{
    Set ..Closed = 1
}

Method IsActive() As %Boolean
{
    Quit '..Closed
}

Storage Default
{
<Data name="RentalContractDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>StartDate</Value>
</Value>
<Value name="3">
<Value>EndDate</Value>
</Value>
<Value name="4">
<Value>AgreementText</Value>
</Value>
<Value name="5">
<Value>Car</Value>
</Value>
<Value name="6">
<Value>Customer</Value>
</Value>
<Value name="7">
<Value>Closed</Value>
</Value>
</Data>
<DataLocation>^Lab5.RentalContractD</DataLocation>
<DefaultData>RentalContractDefaultData</DefaultData>
<IdLocation>^Lab5.RentalContractD</IdLocation>
<IndexLocation>^Lab5.RentalContractI</IndexLocation>
<StreamLocation>^Lab5.RentalContractS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
