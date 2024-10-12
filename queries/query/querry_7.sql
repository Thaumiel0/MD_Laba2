SELECT Name
FROM souvenircategories
WHERE Name IS NOT NULL AND Name != ''
GROUP BY Name
HAVING COUNT(Name) > 1
