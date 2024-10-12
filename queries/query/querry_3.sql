SELECT s.Name, c.Name AS CategoryName, s.Rating
FROM souvenirs AS s
JOIN souvenircategories AS c
	ON s.IdCategory = c.Id
WHERE c.Name = 'Скребки'
ORDER BY s.Rating ASC