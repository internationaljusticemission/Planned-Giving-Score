-- contact_sql for planned giving
SELECT 
    AccountId, 
    Id,
    Phone,
    Email,
    Alumni_IJM__c, 
    Prayer_Partner__c, 
    MailingStreet, 
    MailingCity,
    MailingState,
    MailingPostalCode,
    MailingCountry

FROM NAM.VWContact

WHERE MailingCountry IN ('United States', 'Canada')