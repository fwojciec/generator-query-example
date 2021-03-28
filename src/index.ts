import { DynamoDB } from "aws-sdk"

interface City {
  name: string
  population: number
}

interface CityRepo {
  listCities: () => Promise<City[]>
}

async function main(): Promise<void> {
  const client = new DynamoDB.DocumentClient({ region: "eu-central-1" })
  const repo = new DynamoDBCityRepo(client, "cities")
  const result = await repo.listCities()
  console.log(result)
}

class DynamoDBCityRepo implements CityRepo {
  private client: DynamoDB.DocumentClient
  private tableName: string

  constructor(client: DynamoDB.DocumentClient, tableName: string) {
    this.client = client
    this.tableName = tableName
  }

  async listCities(): Promise<City[]> {
    const result: City[] = []
    const queryInput: DynamoDB.DocumentClient.QueryInput = {
      KeyConditionExpression: "#pk = :pk",
      ExpressionAttributeNames: {
        "#pk": "pk",
      },
      ExpressionAttributeValues: {
        ":pk": "city",
      },
      TableName: this.tableName,
      Limit: 2, // to force pagination
    }
    const pages = this.queryPageGenerator(queryInput)
    for await (const page of pages) {
      result.push(
        ...page.map((item) => ({
          name: item.sk,
          population: item.population,
        }))
      )
    }
    return result
  }

  private async *queryPageGenerator(
    queryInput: DynamoDB.DocumentClient.QueryInput
  ): AsyncGenerator<DynamoDB.DocumentClient.ItemList> {
    let lastEvaluatedKey: DynamoDB.DocumentClient.Key | undefined
    do {
      console.log("executing a query")
      const { Items, LastEvaluatedKey } = await this.client
        .query({ ...queryInput, ExclusiveStartKey: lastEvaluatedKey })
        .promise()
      lastEvaluatedKey = LastEvaluatedKey
      if (Items !== undefined) {
        yield Items
      }
    } while (lastEvaluatedKey !== undefined)
  }
}

main()
