const express = require("express");
const { Client } = require("pg");

const app = express();

app.get("/", async (req, res) => {
  try {
    const client = new Client({
      user: "target_user",
      host: "target",
      database: "target",
      password: "foo",
      port: 5432,
    });
    client.connect();
    const response = await client.query("SELECT * from order_detail");
    res.send(JSON.stringify(response.rows));
  } catch (err) {
    console.log(err);
  }
});

app.listen(3000, () => {
  console.log("Server is running on port 3000");
});
