SELECT sc.ID, sc.Name
FROM souvenircategories sc
WHERE sc.IdParent = (SELECT ID FROM souvenircategories WHERE Name = 'Органайзеры для авто')
