SELECT sp.ID, sp.Data, p.Name AS Provider, ps.Name AS Status
FROM souvenirprocurements sp
JOIN providers p 
	ON sp.IdProvider = p.ID
JOIN  procurementstatuses ps 
	ON sp.IdStatus = ps.ID
WHERE sp.Data BETWEEN '2022-05-01' AND '2024-12-31'
ORDER BY ps.Name