const { MongoClient, ObjectId } = require("mongodb");
const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const url = "mongodb://localhost:27017";
const dbnamemflix = "Sample_mflix";
const dbnameanalytics = "sample_analytics";
const dbnameairbnb = "sample_airbnb";

app.use(bodyParser.json());
(async () => {
  try {
    const client = new MongoClient(url);
    await client.connect();
    console.log("Connected successfully to server");
    const dbmflix = client.db(dbnamemflix);
    const dbanalytics = client.db(dbnameanalytics);
    const dbairbnb = client.db(dbnameairbnb);

    const moviesCollection = dbmflix.collection("movies");
    const commentsCollection = dbmflix.collection("comments");
    const userCollection = dbmflix.collection("users");

    const accountCollection = dbanalytics.collection("accounts");
    const customerCollection = dbanalytics.collection("customers");
    const transactionCollection = dbanalytics.collection("transactions");
    const countedAccount = dbanalytics.collection("countedAccount");
    const transactionByDay = dbanalytics.collection("transactionByDay");
    const customerbytransaction = dbanalytics.collection(
      "rankedCustomersTransaction"
    );

    const listingandreview = dbairbnb.collection("listingandreview");
    //************************MFLIX**************************************//

    app.get("/mflix/getmovies", async (req, res) => {
      const result = await moviesCollection
        .aggregate([
          {
            $lookup: {
              from: "comments",
              localField: "_id",
              foreignField: "movie_id",
              as: "comments",
            },
          },
          { $sort: { released: -1 } },
          {
            $project: {
              _id: 1,
              title: 1,
              released: 1,
              comments: { _id: 1, text: 1, email: 1, name: 1, date: 1 },
            },
          },
        ])
        .limit(20)
        .toArray();
      res.send(result);
    });
    //EXEMPLE DEJSON DE LA REQUETE
    // {
    //     "title": "Avatar 2",
    //     "genre": ["Aventure", "Beu", "Eau"],
    //     "year": 2023,
    //     "plot": "Des gens bleus dans de l'eau bleue alors beaucoup de bleu"

    // }
    app.post("/mflix/addMovie", async (req, res) => {
      console.log(req.body);
      await moviesCollection.insertOne({
        title: req.body.title,
        genre: req.body.genre,
        year: req.body.year,
        plot: req.body.plot,
      });
      res.send("ok");
    });

    app.get("/mflix/getMovieByName/:name", async (req, res) => {
      const movieName = req.params.name;
      const result = await moviesCollection.findOne({
        title: movieName,
      });
      res.send(result);
    });
    app.delete("/mflix/deleteMovie/:name", async (req, res) => {
      const movieName = req.params.name;
      const result = await moviesCollection.deleteOne({
        title: movieName,
      });
      res.send(result);
    });
    //EXEMPLE REQUETE
    //   {
    //     "type":"moviiiie",
    //     "year":2024
    // }
    app.put("/mflix/updateMovie/:name", async (req, res) => {
      console.log(req.body);
      const movieName = req.params.name;
      const result = await moviesCollection.updateOne(
        {
          title: movieName,
        },
        {
          $set: req.body,
        }
      );
      res.send(result);
    });

    app.get("/mflix/getRankedMoviesByCommentsNumber", async (req, res) => {
      const result = await moviesCollection
        .aggregate([
          { $sort: { num_mflix_comments: -1 } },
          { $project: { num_mflix_comments: 0 } },
        ])
        .limit(10)
        .toArray();
      res.send(result);
    });

    //**************************COMMENTS*********************************//
    app.get("/mflix/getComments", async (req, res) => {
      const result = await commentsCollection
        .aggregate([
          { $sort: { date: -1 } },
          {
            $lookup: {
              from: "users",
              localField: "email",
              foreignField: "email",
              as: "user",
            },
          },
          {
            $lookup: {
              from: "movies",
              localField: "movie_id",
              foreignField: "_id",
              as: "movie",
            },
          },
          { $unwind: "$user" },
          { $unwind: "$movie" },
          {
            $project: {
              name: 1,
              email: 1,
              text: 1,
              data: 1,
              user: {
                _id: 1,
                name: 1,
                email: 1,
              },
              movie: {
                _id: 1,
                plot: 1,
                genre: 1,
                runtime: 1,
                num_mflix_comments: 1,
                title: 1,
                fullplot: 1,
                year: 1,
              },
            },
          },
        ])
        .limit(10)
        .toArray();
      res.send(result);
    });

    app.get("/mflix/getComment/:searchterm", async (req, res) => {
      const search = req.params.searchterm;
      const result = await commentsCollection
        .find({
          text: { $regex: search },
        })
        .toArray();
      res.send(result);
    });
    //EXEMPLE JSON REQUETE
    //     {
    //       "name": "Monsieur Rose",
    //       "email": "moi@moi.fr",
    //       "text": "LES GENS SONT BLEUS ET MEME JAUNE !!!",
    //       "movie": "Avatar 2"
    // }
    app.post("/mflix/addComment", async (req, res) => {
      const movie = await moviesCollection.findOne({ title: req.body.movie });
      const comments = {
        name: req.body.name,
        email: req.body.email,
        text: req.body.text,
        movie_id: movie._id,
        date: new Date(),
      };
      await commentsCollection.insertOne(comments);
      res.send("ok");
    });
    app.put("/mflix/updateComment", async (req, res) => {
      const comments = {
        text: req.body.text,
        updatedAt: new Date(),
      };
      const result = await commentsCollection.updateOne(
        {
          _id: new ObjectId(req.body.id),
        },
        {
          $set: comments,
        }
      );
      res.send(result);
    });

    app.delete("/mflix/deleteComment", async (req, res) => {
      const commentid = req.body._id;
      const result = await commentsCollection.deleteOne({
        _id: new ObjectId(commentid),
      });
      res.send(result);
    });

    app.get("/mflix/getUsers", async (req, res) => {
      const result = await commentsCollection
        .aggregate([
          {
            $lookup: {
              from: "users",
              foreignField: "email",
              localField: "email",
              as: "user",
            },
          },
          { $sort: { date: -1 } },
          { $unwind: "$user" },
          {
            $project: {
              _id: 0,
              user: 1,
            },
          },
        ])
        .limit(20)
        .toArray();
      res.json(result);
    });

    app.delete("/mflix/deleteUser", async (req, res) => {
      const userid = req.body._id;
      const result = await userCollection.deleteOne({
        _id: new ObjectId(userid),
      });
      res.send(result);
    });

    //EXEMPLE JSON REQUETE
    //{
    //   "_id": "59b99db4cfa9a34dcd7885b7",
    //   "name" : "Robert Barathon",
    //   "email":"mark_addy@gameofthron.es",
    //   "password": "$2b$12$6vz7wiwO.EI5Rilvq1zUc./9480gb1uPtXcahDxIadgyC3PS8XCUK"
    // }
    app.delete("/mflix/updateUser", async (req, res) => {
      const userid = req.body._id;
      const user = {
        name: req.body.name,
        email: req.body.email,
        password: req.body.password,
      };
      const result = await userCollection.updateOne(
        {
          _id: new ObjectId(userid),
        },
        {
          $set: user,
        }
      );
      res.send(result);
    });

    app.get("/mflix/getUserByMail/:email", async (req, res) => {
      const email = req.params.email;
      const user = await userCollection.findOne({
        email,
      });
      res.send(user);
    });

    //***************************** ANALYTICS ********************************//

    app.post("/analytics/createAccountNumber", async (req, res) => {
      await customerCollection
        .aggregate([
          {
            $project: {
              username: 1,
              name: 1,
              numofaccount: { $size: "$accounts" },
            },
          },
          { $out: { db: "sample_analytics", coll: "countedAccount" } },
        ])
        .toArray();
      res.send("c'est ok brooooooooooooooooo");
    });

    app.get("/analytics/countedAccount", async (req, res) => {
      const result = await countedAccount.find().toArray();
      res.json(result);
    });

    app.post("/analytics/createTransactionsByDay", async (req, res) => {
      await transactionCollection
        .aggregate([
          { $unwind: "$transactions" },
          {
            $group: {
              _id: "$transactions.date",
              transaction: { $push: "$transactions" },
            },
          },
          {
            $out: { db: "sample_analytics", coll: "transactionByDay" },
          },
        ])
        .toArray();

      res.send("THAT'S WORK BRO");
    });

    app.get("/analytics/getTransactionByDay", async (req, res) => {
      const result = await transactionByDay.find().toArray();
      res.json(result);
    });

    app.post("/analytics/createRankedTransaction", async (req, res) => {
      await customerCollection
        .aggregate([
          {
            $lookup: {
              from: "transactions",
              localField: "accounts",
              foreignField: "account_id",
              as: "accounts",
            },
          },
          { $unwind: "$accounts" },
          {
            $group: {
              _id: "$_id",
              transactions: { $push: "$accounts" },
              totaltransaction: { $sum: "$accounts.transaction_count" },
            },
          },
          {
            $lookup: {
              from: "customers",
              localField: "_id",
              foreignField: "_id",
              as: "customer",
            },
          },
          { $unwind: "$customer" },
          {
            $project: {
              _id: 1,
              username: "$customer.username",
              name: "$customer.name",
              totaltransaction: 1,
            },
          },
          { $sort: { totaltransaction: -1 } },
          {
            $out: {
              db: "sample_analytics",
              coll: "rankedCustomersTransaction",
            },
          },
        ])
        .toArray();
      res.send("YEEESSSS BRRRROOOOOOOOO");
    });

    app.get("/analytics/rankCustomers", async (req, res) => {
      const result = await customerbytransaction.find().toArray();
      res.json(result);
    });

    //Fait la relation entre le prix par lit pour une nuit et la note total obtenu sur le logement
    app.get("/airbnb/priceByRate", async (req, res) => {
      const result = await listingandreview
        .aggregate([
          {
            $match: {
              beds: { $ne: 0 },
            },
          },
          {
            $project: {
              name: 1,
              beds: 1,
              price: 1,
              review_scores: {
                review_scores_rating: 1,
              },
            },
          },
          {
            $addFields: {
              price_per_bed: { $divide: ["$price", "$beds"] },
            },
          },
          {
            $group: {
              _id: "$price_per_bed",
              avg_review_score: { $avg: "$review_scores.review_scores_rating" },
            },
          },
          {
            $sort: {
              _id: -1,
            },
          },
        ])
        .toArray();
      res.json(result);
    });
    app.get("/airbnb/bathroombyguest", async (req, res) => {
      const result = await listingandreview
        .aggregate([
          {
            $project: {
              bathrooms: 1,
              guests_included: 1,
            },
          },
          {
            $addFields: {
              bathromm_by_guest: { $divide: ["$bathrooms", "$guests_included"] },
            },
          },
          {
            $sort: {bathromm_by_guest : -1}
          }
        ])
        .toArray();
      res.json(result);
    });
  } catch (e) {
    console.log(e);
  }
})();

app.listen(3000, () => {
  console.log("connect bro");
});
