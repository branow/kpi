Class Lab4.Person Extends (%Persistent, %XML.Adaptor) [ Abstract ]
{

Property Id As %Integer [ Required ];

Index IdIndex On Id [ PrimaryKey ];

Property Name As %String(MAXLEN = 50, MINLEN = 2) [ Required ];

Property Email As %String;

Property Address As Lab4.Address;

Property ContactMethods As array Of %String;

Method ValidateName() As %Boolean
{
    Quit ($Length(..name) >= 2) && ($Length(..name) <= 50)
}

Storage Default
{
<Data name="ContactMethods">
<Attribute>ContactMethods</Attribute>
<Structure>subnode</Structure>
<Subscript>"ContactMethods"</Subscript>
</Data>
<Data name="PersonDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>id</Value>
</Value>
<Value name="3">
<Value>name</Value>
</Value>
<Value name="4">
<Value>email</Value>
</Value>
<Value name="5">
<Value>Id</Value>
</Value>
<Value name="6">
<Value>Name</Value>
</Value>
<Value name="7">
<Value>Email</Value>
</Value>
<Value name="8">
<Value>Address</Value>
</Value>
</Data>
<DataLocation>^Lab4.PersonD</DataLocation>
<DefaultData>PersonDefaultData</DefaultData>
<IdLocation>^Lab4.PersonD</IdLocation>
<IndexLocation>^Lab4.PersonI</IndexLocation>
<StreamLocation>^Lab4.PersonS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
