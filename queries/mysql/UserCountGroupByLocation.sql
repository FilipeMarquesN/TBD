SELECT COUNT(User-ID),AVG(Age), Location
FROM USERS u
WHERE u.Age > 0
GROUP BY u.Location;
