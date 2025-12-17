Class Lab5.Payment Extends (%Persistent, %Library.Populate)
{

Property Amount As %Decimal(MINVAL = 0.0) [ Required ];

Property PaymentDate As %Date;

Property Method As %String;

Property Processed As %Boolean;

Relationship RentalContract As Lab5.RentalContract [ Cardinality = parent, Inverse = Payments ];

Method Process() As %Boolean
{
    Set ..Processed = 1
    Quit 1
}

Method Refund() As %Boolean
{
    Set ..Processed = 0
    Quit 1
}

Storage Default
{
<Data name="PaymentDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>Amount</Value>
</Value>
<Value name="3">
<Value>PaymentDate</Value>
</Value>
<Value name="4">
<Value>Method</Value>
</Value>
<Value name="5">
<Value>Processed</Value>
</Value>
</Data>
<DataLocation>{%%PARENT}("Payments")</DataLocation>
<DefaultData>PaymentDefaultData</DefaultData>
<IdLocation>^Lab5.RentalContractC("Payments")</IdLocation>
<IndexLocation>^Lab5.PaymentI</IndexLocation>
<StreamLocation>^Lab5.PaymentS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
