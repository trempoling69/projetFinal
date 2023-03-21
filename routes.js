

const routes = async (req,res) => {
    const result = await moviesCollection.findOne();
      res.json(result);
}

module.exports = {routes}