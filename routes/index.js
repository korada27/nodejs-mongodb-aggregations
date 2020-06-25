var express = require('express');
var router = express.Router();
var mongodb = require("mongodb");
var MongoClient = mongodb.MongoClient;

// Testing URL 
var url = 'mongodb://madhu:madhu123@ds149984.mlab.com:49984/dev';

router.get('/task1', function(req, res, next) {
  let paramvalue = parseInt(req.query.n) || 0;

	MongoClient.connect(url, function (err, client) {
    if(err){
      console.log("Error in COnnecting DB", err)
    }else{
      console.log("Connected to DB");
     
      let database = client.db('dev');

      database.collection('authors').find({ totalAwards: { $gt: paramvalue } })
      .toArray((err, results) => {
          if(err) throw err;
          client.close();
          res.send(results)
      })
    }
  })
});

router.get('/task2', function(req, res, next) {
  let paramvalue = parseInt(req.query.y) || 0;

	MongoClient.connect(url, function (err, client) {
    if(err){
      console.log("Error in COnnecting DB", err)
    }else{
      console.log("Connected to DB");
     
      let database = client.db('dev');

      database.collection('awards').aggregate([
        { $match: { year: paramvalue } },
        {
          $lookup: {
            from : 'authors',
            localField: "authorid",
            foreignField: "_id",
            as: "authors"
          }
        },
      ])
      .toArray((err, results) => {
          if(err) throw err;
          client.close();
          res.send(results)
      })
    }
  })
});

router.get('/task3', function(req, res, next) {
  let paramvalue = parseInt(req.query.n) || 0;

	MongoClient.connect(url, function (err, client) {
    if(err){
      console.log("Error in COnnecting DB", err)
    }else{
      console.log("Connected to DB");
     
      let database = client.db('dev');

      database.collection('sales').aggregate([
        {
          $group:
          {
          _id: "$authorid",
          "totalProfit": {
            $sum : {
              $multiply:["$price","$quantity"]
            },
           
          },
          "totalBooksSold": { $sum: 1 }
        }},
        {
          $lookup: {
            from : 'authors',
            localField: "_id",
            foreignField: "_id",
            as: "authors"
          }
        },
        {
          $unwind: { path: "$authors", preserveNullAndEmptyArrays: true }
        },
        {
          $project:{
            "_id":1,
            "name":"$authors.name",
            "totalBooksSold":1,
            "totalProfit":1
          }
        }
      ])


      .toArray((err, results) => {
          if(err) throw err;
          client.close();
          res.send(results)
      })
    }
  })
});

router.get('/task4', function(req, res, next) {
  let birthDate = new Date(req.query.birthDate) || 0;
  let totalPrice = parseInt(req.query.totalPrice) || 0

	MongoClient.connect(url, function (err, client) {
    if(err){
      console.log("Error in COnnecting DB", err)
    }else{
      console.log("Connected to DB");
     
      let database = client.db('dev');

      database.collection('sales').aggregate([
        {
          $group:
          {
          _id: "$authorid",
          "totalProfit": {
            $sum : {
              $multiply:["$price","$quantity"]
            },
           
          },
          "totalBooksSold": { $sum: 1 }
        }},
        {
          $lookup: {
            from : 'authors',
            localField: "_id",
            foreignField: "_id",
            as: "authors"
          }
        },
        {
          $unwind: { path: "$authors", preserveNullAndEmptyArrays: true }
        },
        {
          $project:{
            "_id":1,
            "name":"$authors.name",
            "totalBooksSold":1,
            "totalProfit":1,
            "dob":"$authors.dob",
          }
        },
        {
          $redact: {
            $cond: {
              if: {
                $and:[
                    {$gte:["$dob", birthDate]},
                    {$gte: ["$totalProfit", totalPrice ]}
                ]
            },
              then: "$$DESCEND",
              else: "$$PRUNE"
            }
          }
        }
      ])

      .toArray((err, results) => {
          if(err) throw err;
          client.close();
          res.send(results)
      })
    }
  })
});

module.exports = router;
