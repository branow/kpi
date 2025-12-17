Class Lab5.CarModel Extends (%Persistent, %Library.Populate)
{

Property Brand As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetCarBrand()");

Property ModelName As %String(POPSPEC = "##class(Lab5.PopulateHelper).GetCarModel()");

Property Year As %Integer(MAXVAL = 2025, MINVAL = 2000);

Property Features As list Of %String;

Method OnPopulate() As %Status
{
    Do ..Features.Clear()
    
    Set count = $Random(5) + 1
    
    For i=1:1:count {
        Set feature = ##class(Lab5.PopulateHelper).GetCarFeature()
        Do ..Features.Insert(feature)
    }
    
    Quit $$$OK
}

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
<Value>Brand</Value>
</Value>
<Value name="3">
<Value>ModelName</Value>
</Value>
<Value name="4">
<Value>Year</Value>
</Value>
<Value name="5">
<Value>Features</Value>
</Value>
</Data>
<DataLocation>^Lab5.CarModelD</DataLocation>
<DefaultData>CarModelDefaultData</DefaultData>
<IdLocation>^Lab5.CarModelD</IdLocation>
<IndexLocation>^Lab5.CarModelI</IndexLocation>
<StreamLocation>^Lab5.CarModelS</StreamLocation>
<Type>%Storage.Persistent</Type>
}

}
