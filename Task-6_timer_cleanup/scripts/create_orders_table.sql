CREATE TABLE Orders (
  id NVARCHAR(100) PRIMARY KEY,
  customerId NVARCHAR(100),
  name NVARCHAR(255),
  price DECIMAL(18,2),
  createdOn DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updatedOn DATETIME2 NULL,
  otherJson NVARCHAR(MAX) NULL -- store any other fields as JSON text
);

-- Add index on createdOn for efficient range scan
CREATE INDEX IDX_Orders_CreatedOn ON Orders(createdOn);
