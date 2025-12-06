Class Lab4.Run Extends %RegisteredObject
{

ClassMethod Setup()
{
    // --- Create Car Models ---
    Set m1 = ##class(Lab4.CarModel).%New()
    Set m1.ModelId = 1, m1.Brand = "Toyota", m1.ModelName = "Corolla", m1.Year = 2022
    Do m1.Features.Insert("Air Conditioning"), m1.Features.Insert("Bluetooth")
    Do m1.%Save()
    Write "Created CarModel: ", m1.ModelName, !

    Set m2 = ##class(Lab4.CarModel).%New()
    Set m2.ModelId = 2, m2.Brand = "Tesla", m2.ModelName = "Model 3", m2.Year = 2024
    Do m2.Features.Insert("Autopilot"), m2.Features.Insert("Heated Seats")
    Do m2.%Save()
    Write "Created CarModel: ", m2.ModelName, !

    // --- Create Cars ---
    Set car1 = ##class(Lab4.Car).%New()
    Set car1.Vin = "VIN12345", car1.LicensePlate = "ABC-111", car1.Status = "Available"
    Set car1.Mileage = 12000, car1.DailyRate = 45.5, car1.Model = m1
    Do car1.%Save()
    Write "Created Car: ", car1.LicensePlate, !

    Set car2 = ##class(Lab4.Car).%New()
    Set car2.Vin = "VIN67890", car2.LicensePlate = "XYZ-222", car2.Status = "Available"
    Set car2.Mileage = 8000, car2.DailyRate = 120.0, car2.Model = m2
    Do car2.%Save()
    Write "Created Car: ", car2.LicensePlate, !

    // --- Create Customers ---
    Set c1 = ##class(Lab4.Customer).%New()
    Set c1.Id = 1, c1.Name = "Alice Brown", c1.Email = "alice@example.com", c1.DriverLicenseNumber = "D12345"
    Set c1.Address = ##class(Lab4.Address).%New()
    Set c1.Address.Street = "123 Oak St", c1.Address.City = "Springfield", c1.Address.ZipCode = "12345"
    Do c1.ContactMethods.SetAt("555-0101", "Mobile")
    Do c1.ContactMethods.SetAt("AB111", "Pager")
    Do c1.%Save()
    Write "Created Customer: ", c1.Name, !

    Set c2 = ##class(Lab4.Customer).%New()
    Set c2.Id = 2, c2.Name = "Bob Smith", c2.Email = "bob@example.com", c2.DriverLicenseNumber = "X98765"
    Set c2.Address = ##class(Lab4.Address).%New()
    Set c2.Address.Street = "45 Elm Ave", c2.Address.City = "Shelbyville", c2.Address.ZipCode = "54321"
    Do c2.ContactMethods.SetAt("555-0202", "Home")
    Do c2.%Save()
    Write "Created Customer: ", c2.Name, !

    // --- Create Employees ---
    Set e1 = ##class(Lab4.Employee).%New()
    Set e1.Id = 10, e1.Name = "Eve Johnson", e1.Email = "eve@iris.com"
    Set e1.EmployeeId = "EMP001", e1.Position = "Manager", e1.HireDate = $ZDATEH("2022-01-10", 3)
    Set e1.Address = ##class(Lab4.Address).%New()
    Set e1.Address.Street = "700 Corporate Dr", e1.Address.City = "Capital City", e1.Address.ZipCode = "60001"
    Do e1.ContactMethods.SetAt("555-0303", "WorkPhone")
    Do e1.%Save()
    Write "Created Employee: ", e1.Name, !

    Set e2 = ##class(Lab4.Employee).%New()
    Set e2.Id = 11, e2.Name = "Tom White", e2.Email = "tom@iris.com"
    Set e2.EmployeeId = "EMP002", e2.Position = "Mechanic", e2.HireDate = $ZDATEH("2023-06-20", 3)
    Set e2.Address = ##class(Lab4.Address).%New()
    Set e2.Address.Street = "22 Industrial Pkwy", e2.Address.City = "Steel Town", e2.Address.ZipCode = "60002"
    Do e2.ContactMethods.SetAt("555-0404", "WorkPhone")
    Do e2.ContactMethods.SetAt("twhite@iris.com", "WorkEmail")
    Do e2.%Save()
    Write "Created Employee: ", e2.Name, !

    // --- Create Rental Contracts (Requirement: At least 2 objects) ---
    Set r1 = ##class(Lab4.RentalContract).%New()
    Set r1.ContractId = 100
    Set r1.StartDate = $ZDATEH("2025-10-01", 3)
    Set r1.EndDate = $ZDATEH("2025-10-05", 3)
    Set r1.Car = car1, r1.Customer = c1
    Do r1.%Save()
    Write "Created RentalContract: ", r1.ContractId, !

    Set r2 = ##class(Lab4.RentalContract).%New()
    Set r2.ContractId = 101
    Set r2.StartDate = $ZDATEH("2025-11-15", 3)
    Set r2.EndDate = $ZDATEH("2025-11-20", 3)
    Set r2.Car = car2, r2.Customer = c2
    Do r2.%Save()
    Write "Created RentalContract: ", r2.ContractId, !

    // --- Create Payments (Requirement: At least 2 objects) ---
    Set p1 = ##class(Lab4.Payment).%New()
    Set p1.PaymentId = 9001, p1.Amount = 180.0, p1.PaymentDate = $ZDATEH("2025-10-01",3), p1.Method = "Credit Card"
    Set p1.RentalContract = r1
    Do p1.%Save()
    Write "Added Payment: ", p1.PaymentId, !

    Set p2 = ##class(Lab4.Payment).%New()
    Set p2.PaymentId = 9002, p2.Amount = 600.0, p2.PaymentDate = $ZDATEH("2025-11-15",3), p2.Method = "Bank Transfer"
    Set p2.RentalContract = r2
    Do p2.%Save()
    Write "Added Payment: ", p2.PaymentId, !

    Write !, "=== Demo Data Setup Complete ===", !
}

ClassMethod Reset()
{
    Do ##class(Lab4.Car).%KillExtent()
    Do ##class(Lab4.CarModel).%KillExtent()
    Do ##class(Lab4.Customer).%KillExtent()
    Do ##class(Lab4.Employee).%KillExtent()
    Do ##class(Lab4.Payment).%KillExtent()
    Do ##class(Lab4.RentalContract).%KillExtent()
    Write "All demo data removed.", !
}

}
