import pandas as pd


df = pd.read_csv("titanic.csv")
print(df.head())

# วิธีการเรียกดูค่าใน Column
# df["Survived"]
# df.Survived

df.info()

age_not_null = df.Age.notnull()
dq_age = age_not_null.sum() / len(df)
print(f"Data Quality of Age: {dq_age}")

cabin_not_null = df.Cabin.notnull()
dq_cabin = cabin_not_null.sum() / len(df)
print(f"Data Quality of Cabin: {dq_cabin}")

embarked_not_null = df.Embarked.notnull()
dq_embarked = embarked_not_null.sum() / len(df)
print(f"Data Quality of Embarked: {dq_embarked}")

passengerId_not_null = df.PassengerId.notnull()
dq_passengerId = passengerId_not_null.sum() / len(df)
print(f"Data Quality of passengerId: {dq_passengerId}")

survived_not_null = df.Survived.notnull()
dq_survived = survived_not_null.sum() / len(df)
print(f"Data Quality of survived: {dq_survived}")

pclass_not_null = df.Pclass.notnull()
dq_pclass = pclass_not_null.sum() / len(df)
print(f"Data Quality of pclass: {dq_pclass}")

name_not_null = df.Name.notnull()
dq_name = name_not_null.sum() / len(df)
print(f"Data Quality of name: {dq_name}")

sex_not_null = df.Sex.notnull()
dq_sex = sex_not_null.sum() / len(df)
print(f"Data Quality of sex: {dq_sex}")

sibSp_not_null = df.SibSp.notnull()
dq_sibSp = sibSp_not_null.sum() / len(df)
print(f"Data Quality of sibSp: {dq_sibSp}")

parch_not_null = df.Parch.notnull()
dq_parch = parch_not_null.sum() / len(df)
print(f"Data Quality of parch: {dq_parch}")

ticket_not_null = df.Ticket.notnull()
dq_ticket = ticket_not_null.sum() / len(df)
print(f"Data Quality of ticket: {dq_ticket}")

ticket_not_null = df.Ticket.notnull()
dq_ticket = ticket_not_null.sum() / len(df)
print(f"Data Quality of ticket: {dq_ticket}")

ticket_not_null = df.Ticket.notnull()
dq_ticket = ticket_not_null.sum() / len(df)
print(f"Data Quality of ticket: {dq_ticket}")

print(f"Completeness: {(dq_age + dq_cabin + dq_embarked + dq_passengerId + dq_survived + dq_pclass + dq_name + dq_sex + dq_sibSp + dq_parch + dq_ticket) / 8}")