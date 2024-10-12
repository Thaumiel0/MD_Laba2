SELECT p.Name FROM Souvenirs AS s
JOIN souvenircategories AS c
	ON s.IdCategory = c.Id
JOIN procurementsouvenirs AS ps
	ON s.Id = ps.IdSouvenir
JOIN souvenirprocurements AS sp
	ON ps.IdProcurement = sp.Id
JOIN providers AS p
	ON sp.IdProvider = p.Id
WHERE c.Name = 'Деловые подарки'