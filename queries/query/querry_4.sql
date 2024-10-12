SELECT sc.ID, sc.Name
FROM souvenircategories sc
WHERE sc.IdParent IN (SELECT ID FROM souvenircategories WHERE Name = 'Дорожные сумки')