Class Lab4.Payment Extends %Persistent
{

Property PaymentId As %Integer [ Required ];

Index PaymentIdIndex On PaymentId [ PrimaryKey ];

Property Amount As %Decimal(MINVAL = 0.0) [ Required ];

Property PaymentDate As %Date;

Property Method As %String;

Property Processed As %Boolean;

Relationship RentalContract As Lab4.RentalContract [ Cardinality = parent, Inverse = Payments ];

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
<Value>paymentId</Value>
</Value>
<Value name="3">
<Value>amount</Value>
</Value>
<Value name="4">
<Value>paymentDate</Value>
</Value>
<Value name="5">
<Value>method</Value>
</Value>
<Value name="6">
<Value>processed</Value>
</Value>
<Value name="7">
<Value>PaymentId</Value>
</Value>
<Value name="8">
<Value>Amount</Value>
</Value>
<Value name="9">
<Value>PaymentDate</Value>
</Value>
<Value name="10">
<Value>Method</Value>
</Value>
<Value name="11">
<Value>Processed</Value>
</Value>
</Data>
<DataLocation>^Lab4.PaymentD</DataLocation>
<DefaultData>PaymentDefaultData</DefaultData>
<IdLocation>^Lab4.PaymentD</IdLocation>
<IndexLocation>^Lab4.PaymentI</IndexLocation>
<StreamLocation>^Lab4.PaymentS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
