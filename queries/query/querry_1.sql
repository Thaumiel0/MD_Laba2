SELECT s.Name, m.Name as Material 
FROM souvenirs AS s
JOIN souvenirmaterials AS m
ON s.IdMaterial = m.Id
WHERE m.Name = 'металл'