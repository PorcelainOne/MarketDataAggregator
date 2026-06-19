using MarketDataAggregator.Simulator;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://localhost:5071");

var app = builder.Build();

app.UseWebSockets();
app.MapGet("/", () => Results.Ok("Market data simulator"));
SimulatedExchangeServer.Map(app);

app.Run();
