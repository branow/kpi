Class Lab5.Tests.MainTest Extends %UnitTest.TestCase
{

Method OnBeforeAllTests() As %Status
{
    Do ##class(Lab5.Payment).%DeleteExtent()
    Do ##class(Lab5.RentalContract).%DeleteExtent()
    Do ##class(Lab5.Car).%DeleteExtent()
    Do ##class(Lab5.CarModel).%DeleteExtent()
    Do ##class(Lab5.Customer).%DeleteExtent()
    Do ##class(Lab5.Employee).%DeleteExtent()
    Quit $$$OK
}

Method OnAfterAllTests() As %Status
{
    Do ##class(Lab5.Payment).%DeleteExtent()
    Do ##class(Lab5.RentalContract).%DeleteExtent()
    Do ##class(Lab5.Car).%DeleteExtent()
    Do ##class(Lab5.CarModel).%DeleteExtent()
    Do ##class(Lab5.Customer).%DeleteExtent()
    Do ##class(Lab5.Employee).%DeleteExtent()
    Quit $$$OK
}

Method TestContractConstraints()
{
    Set contract = ##class(Lab5.RentalContract).%New()
    Do $$$AssertTrue($isobject(contract), "RentalContract object created")

    Set status = contract.%Save()
    Do $$$AssertStatusNotOK(status, "Save failed because dates are required")

    Set contract.StartDate = +$H
    Set contract.EndDate = +$H + 5

    Set status = contract.%Save()
    Do $$$AssertStatusOK(status, "Contract saved successfully with valid dates")
    Do $$$AssertNotEquals(contract.%Id(), "", "Contract received an ID after saving")

    Do $$$AssertEquals(contract.RentalDays, 6, "RentalDays calculated correctly (EndDate - StartDate + 1)")
}

Method TestPaymentValidation()
{
    Set pay = ##class(Lab5.Payment).%New()

    Set status = pay.%Save()
    Do $$$AssertStatusNotOK(status, "Saving Payment failed without an Amount (Required)")

    Set pay.Amount = -100
    Set status = pay.%Save()
    Do $$$AssertStatusNotOK(status, "Saving failed: Amount cannot be negative")

    Set pay.Amount = 150

    Set tempContract = ##class(Lab5.RentalContract).%New()
    Set tempContract.StartDate = +$H
    Set tempContract.EndDate = +$H + 1
    Do tempContract.%Save()

    Set pay.RentalContract = tempContract

    Set status = pay.%Save()
    Do $$$AssertStatusOK(status, "Payment saved successfully with a valid amount AND linked to a contract")
}

Method TestRelationshipAndTotals()
{
    Set cust = ##class(Lab5.Customer).%New()
    Set cust.Name = "Test Customer"
    Set cust.DriverLicenseNumber = "ABC12345" 
    Do $$$AssertStatusOK(cust.%Save(), "Customer created successfully")
    
    Set contract = ##class(Lab5.RentalContract).%New()
    Set contract.StartDate = +$H
    Set contract.EndDate = +$H + 3
    Set contract.Customer = cust
    Do contract.%Save()
    Set contractId = contract.%Id()
    
    Set p1 = ##class(Lab5.Payment).%New()
    Set p1.Amount = 100
    Do contract.Payments.Insert(p1)
    
    Set p2 = ##class(Lab5.Payment).%New()
    Set p2.Amount = 250
    Do contract.Payments.Insert(p2)
    
    Do $$$AssertStatusOK(contract.%Save(), "Contract with payments saved successfully")
    
    Set openContract = ##class(Lab5.RentalContract).%OpenId(contractId)
    
    Do $$$AssertEquals(openContract.Customer.Name, "Test Customer", "Customer linked correctly")
    Do $$$AssertEquals(openContract.Payments.Count(), 2, "Number of payments equals 2")
    Do $$$AssertEquals(openContract.TotalPayment, 350, "TotalPayment calculated correctly")
}

Method TestCascadingDelete()
{
    Set contract = ##class(Lab5.RentalContract).%New()
    Set contract.StartDate = +$H
    Set contract.EndDate = +$H
    
    Set pay = ##class(Lab5.Payment).%New()
    Set pay.Amount = 500
    Do contract.Payments.Insert(pay)
    
    Do contract.%Save()
    
    Set contractId = contract.%Id()
    Set payId = contract.Payments.GetAt(1).%Id()
    
    Do $$$AssertTrue(##class(Lab5.RentalContract).%ExistsId(contractId), "Contract exists")
    Do $$$AssertTrue(##class(Lab5.Payment).%ExistsId(payId), "Payment exists")
    
    Set status = ##class(Lab5.RentalContract).%DeleteId(contractId)
    Do $$$AssertStatusOK(status, "Contract deleted successfully")
    
    Do $$$AssertNotTrue(##class(Lab5.RentalContract).%ExistsId(contractId), "Contract no longer exists")
    Do $$$AssertNotTrue(##class(Lab5.Payment).%ExistsId(payId), "Payment deleted via cascading")
}

}
