Class Lab6.Tests.MainTest Extends %UnitTest.TestCase
{

/// Цей метод викликається перед запуском усіх тестів.
/// Очищаємо базу, щоб тести не залежали від попередніх даних.
Method OnBeforeAllTests() As %Status
{
    // Важливий порядок: спочатку дочірні, потім батьківські (хоча KillExtent потужний, порядок - гарна звичка)
    Do ##class(Lab6.Payment).%KillExtent()
    Do ##class(Lab6.RentalContract).%KillExtent()
    Do ##class(Lab6.Customer).%KillExtent()
    Do ##class(Lab6.Car).%KillExtent()
    Quit $$$OK
}

Method OnAfterAllTests() As %Status
{
    Do ##class(Lab6.Payment).%KillExtent()
    Do ##class(Lab6.RentalContract).%KillExtent()
    Do ##class(Lab6.Customer).%KillExtent()
    Do ##class(Lab6.Car).%KillExtent()
    Quit $$$OK
}

/// Тест 1: Перевірка створення RentalContract та обов'язкових полів
Method TestContractConstraints()
{
    Set contract = ##class(Lab6.RentalContract).%New()
    Do $$$AssertTrue($isobject(contract), "Створено об'єкт RentalContract")
    
    // Спроба зберегти пустий контракт
    Set status = contract.%Save()
    Do $$$AssertStatusNotOK(status, "Збереження не вдалося, оскільки дати є обов'язковими")
    
    // Заповнюємо обов'язкові поля
    Set contract.StartDate = +$H
    Set contract.EndDate = +$H + 5
    
    Set status = contract.%Save()
    Do $$$AssertStatusOK(status, "Успішне збереження Contract з коректними датами")
    Do $$$AssertNotEquals(contract.%Id(), "", "Contract отримав ID після збереження")
    
    // Перевірка обчислюваного поля RentalDays
    // 5 днів різниці + 1 (включно) = 6 днів
    Do $$$AssertEquals(contract.RentalDays, 6, "RentalDays розраховано коректно (EndDate - StartDate + 1)")
}

/// Тест 2: Перевірка валідації Payment (Amount >= 0)
Method TestPaymentValidation()
{
    // 1. Створюємо платіж
    Set pay = ##class(Lab6.Payment).%New()
    
    // --- Негативні тести (тут батько ще не обов'язковий, бо збереження все одно не пройде через інші причини) ---
    
    // Перевірка на пусте значення
    Set status = pay.%Save()
    Do $$$AssertStatusNotOK(status, "Збереження Payment не вдалося без суми (Required)")
    
    // Перевірка на мінусове значення
    Set pay.Amount = -100
    Set status = pay.%Save()
    Do $$$AssertStatusNotOK(status, "Збереження не вдалося, Amount не може бути від'ємним")
    
    // --- Позитивний тест (ТУТ БУЛА ПОМИЛКА) ---
    
    // 2. Встановлюємо правильну суму
    Set pay.Amount = 150
    
    // 3. !!! ВАЖЛИВО: Створюємо "Батька" для платежу !!!
    Set tempContract = ##class(Lab6.RentalContract).%New()
    Set tempContract.StartDate = +$H
    Set tempContract.EndDate = +$H + 1
    Do tempContract.%Save()
    
    // 4. Прив'язуємо платіж до контракту
    Set pay.RentalContract = tempContract
    
    // 5. Тепер зберігаємо - має бути ОК
    Set status = pay.%Save()
    Do $$$AssertStatusOK(status, "Успішне збереження Payment з коректною сумою ТА прив'язкою до контракту")
}

/// Тест 3: Перевірка заповнення відношень та обчислення TotalPayment
Method TestRelationshipAndTotals()
{
    // 1. Створюємо Клієнта (незалежний об'єкт)
    Set cust = ##class(Lab6.Customer).%New()
    Set cust.Name = "Test Customer"
    
    // --- ВИПРАВЛЕННЯ ТУТ ---
    // Додаємо обов'язкове поле довжиною >= 5 символів
    Set cust.DriverLicenseNumber = "ABC12345" 
    // -----------------------
    
    // Зберігаємо клієнта. 
    // Якщо тут впаде помилка - перевірте, чи є ще обов'язкові поля у Lab6.Customer
    Do $$$AssertStatusOK(cust.%Save(), "Клієнт успішно створений")
    
    // 2. Створюємо Контракт
    Set contract = ##class(Lab6.RentalContract).%New()
    Set contract.StartDate = +$H
    Set contract.EndDate = +$H + 3
    Set contract.Customer = cust  // Прив'язка клієнта
    Do contract.%Save()
    Set contractId = contract.%Id()
    
    // 3. Створюємо Платежі (Child)
    Set p1 = ##class(Lab6.Payment).%New()
    Set p1.Amount = 100
    Do contract.Payments.Insert(p1) // Вставка в relationship
    
    Set p2 = ##class(Lab6.Payment).%New()
    Set p2.Amount = 250
    Do contract.Payments.Insert(p2)
    
    // Зберігаємо батька, діти збережуться автоматично
    Do $$$AssertStatusOK(contract.%Save(), "Контракт з платежами збережено")
    
    // 4. ПЕРЕВІРКА
    // Перевідкриваємо об'єкт, щоб переконатись, що дані зчитані з бази
    Set openContract = ##class(Lab6.RentalContract).%OpenId(contractId)
    
    // Перевірка Customer
    Do $$$AssertEquals(openContract.Customer.Name, "Test Customer", "Клієнт прив'язався коректно")
    
    // Перевірка кількості платежів
    Do $$$AssertEquals(openContract.Payments.Count(), 2, "Кількість платежів дорівнює 2")
    
    // Перевірка TotalPayment
    Do $$$AssertEquals(openContract.TotalPayment, 350, "TotalPayment пораховано вірно")
}

/// Тест 4: Перевірка каскадного видалення (Parent-Children)
Method TestCascadingDelete()
{
    // Підготовка: Контракт + Платіж
    Set contract = ##class(Lab6.RentalContract).%New()
    Set contract.StartDate = +$H
    Set contract.EndDate = +$H
    
    Set pay = ##class(Lab6.Payment).%New()
    Set pay.Amount = 500
    Do contract.Payments.Insert(pay)
    
    Do contract.%Save()
    
    Set contractId = contract.%Id()
    Set payId = contract.Payments.GetAt(1).%Id()
    
    // Переконуємось, що об'єкти існують
    Do $$$AssertTrue(##class(Lab6.RentalContract).%ExistsId(contractId), "Контракт існує")
    Do $$$AssertTrue(##class(Lab6.Payment).%ExistsId(payId), "Платіж існує")
    
    // Дія: Видаляємо Контракт
    Set status = ##class(Lab6.RentalContract).%DeleteId(contractId)
    Do $$$AssertStatusOK(status, "Контракт успішно видалено")
    
    // Перевірка: Платіж має зникнути автоматично
    Do $$$AssertNotTrue(##class(Lab6.RentalContract).%ExistsId(contractId), "Контракт більше не існує")
    Do $$$AssertNotTrue(##class(Lab6.Payment).%ExistsId(payId), "Платіж видалено каскадно")
}

}
