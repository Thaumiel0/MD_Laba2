SELECT s.Name, sp.Data FROM Souvenirs AS s
JOIN procurementsouvenirs AS ps
	ON s.Id = ps.IdSouvenir
JOIN souvenirprocurements AS sp
	ON ps.IdProcurement = sp.Id
WHERE sp.Data > '2020-01-12' AND sp.Data < '2024-10-24'