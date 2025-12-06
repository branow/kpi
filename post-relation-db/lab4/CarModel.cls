Class Lab4.CarModel Extends %Persistent
{

Property ModelId As %Integer [ Required ];

Index ModelIdIndex On ModelId [ Unique ];

Property Brand As %String;

Property ModelName As %String;

Property Year As %Integer(MAXVAL = 2025, MINVAL = 2000);

Property Features As list Of %String;

Method AddFeature(f As %String)
{
    Do ..Features.Insert(f)
}

Method RemoveFeature(f As %String)
{
    Set i = ..Features.Find(f)
    If i>0 Do ..Features.RemoveAt(i)
}

Storage Default
{
<Data name="CarModelDefaultData">
<Value name="1">
<Value>%%CLASSNAME</Value>
</Value>
<Value name="2">
<Value>modelId</Value>
</Value>
<Value name="3">
<Value>brand</Value>
</Value>
<Value name="4">
<Value>modelName</Value>
</Value>
<Value name="5">
<Value>year</Value>
</Value>
<Value name="6">
<Value>features</Value>
</Value>
<Value name="7">
<Value>ModelId</Value>
</Value>
<Value name="8">
<Value>Brand</Value>
</Value>
<Value name="9">
<Value>ModelName</Value>
</Value>
<Value name="10">
<Value>Year</Value>
</Value>
<Value name="11">
<Value>Features</Value>
</Value>
</Data>
<DataLocation>^Lab4.CarModelD</DataLocation>
<DefaultData>CarModelDefaultData</DefaultData>
<IdLocation>^Lab4.CarModelD</IdLocation>
<IndexLocation>^Lab4.CarModelI</IndexLocation>
<StreamLocation>^Lab4.CarModelS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
