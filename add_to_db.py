# Setup

from src import app
from src.models import User, db

with app.app_context():
    # u1 = User(account="Test", username="jsc", password="password")
    # db.session.add(u1)
    # db.session.commit()

    db.create_all()
    u1 = User(account="Test", username="jsc", password="password")
    db.session.add(u1)
    db.session.commit()

    # i1 = Item(
    #     name="Iphone 10", description="description", barcode="123456789123", price=800
    # )
    # db.session.add(i1)

    # i2 = Item(
    #     name="Laptop", description="description2", barcode="123356789123", price=1000
    # )
    # db.session.add(i2)

    # item1 = Item.query.filter_by(name="Iphone 10").first()

    # item1.owner = User.query.filter_by(username="jsc").first().id

    # db.session.add(item1)
    # db.session.commit()

    # db.create_all()
    # item1 = Item(
    #     name="IPhone 10", price=500, barcode="283749165873", description="desc"
    # )
    # item2 = Item(
    #     name="Laptop", price=600, barcode="283742365873", description="description"
    # )
    # db.session.add(item1)
    # db.session.add(item2)
    # db.session.commit()
