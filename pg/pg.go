package pg

import (
	"fmt"
	"math"

	_ "github.com/lib/pq"
)

type Data struct {
	Index  string `json:"_index"`
	Type   string `json:"_type"`
	ID     string `json:"_id"`
	Score  int    `json:"_score"`
	Source Source `json:"_source"`
}

type Source struct {
	OsmID      int     `json:"osm_id" db:"osm_id"`
	Name       string  `json:"name"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
	Level0     string  `json:"level_0"`
	Level0Bn   string  `json:"level_0_bn"`
	Level1     string  `json:"level_1"`
	Level1Bn   string  `json:"level_1_bn"`
	Level2     string  `json:"level_2"`
	Level2Bn   string  `json:"level_2_bn"`
	Level3     string  `json:"level_3"`
	Level3Bn   string  `json:"level_3_bn"`
	Level4     string  `json:"level_4"`
	Level4Bn   string  `json:"level_4_bn"`
	Level5     string  `json:"level_5"`
	Level5Bn   string  `json:"level_5_bn"`
	Area       string  `json:"area"`
	AreaBn     string  `json:"area_bn"`
	Popularity int     `json:"popularity"`
	Catagory   string  `json:"catagory"`
	Location   struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"location"`
}

type Schema struct {
	Id           string  `json:"id" db:"id"`
	Name         string  `json:"name" db:"name"`
	Originalname string  `json:"originalname" db:"originalname"`
	Level_0      string  `json:"level_0" db:"level_0"`
	Level_1      string  `json:"level_1" db:"level_1"`
	Level_2      string  `json:"level_2" db:"level_2"`
	Level_3      string  `json:"level_3" db:"level_3"`
	Level_4      string  `json:"level_4" db:"level_4"`
	Level_5      string  `json:"level_5" db:"level_5"`
	Level_0_bn   string  `json:"level_0_bn" db:"level_0_bn"`
	Level_1_bn   string  `json:"level_1_bn" db:"level_1_bn"`
	Level_2_bn   string  `json:"level_2_bn" db:"level_2_bn"`
	Level_3_bn   string  `json:"level_3_bn" db:"level_3_bn"`
	Level_4_bn   string  `json:"level_4_bn" db:"level_4_bn"`
	Level_5_bn   string  `json:"level_5_bn" db:"level_5_bn"`
	Area         string  `json:"area" db:"area"`
	Area_bn      string  `json:"area_bn" db:"area_bn"`
	Popularity   int     `json:"popularity" db:"popularity"`
	Road         string  `json:"road" db:"road"`
	City         string  `json:"city" db:"city"`
	Country      string  `json:"country" db:"country"`
	Catagory     string  `json:"catagory" db:"catagory"`
	Ptype        string  `json:"ptype" db:"ptype"`
	Updatedname  string  `json:"updatedname" db:"updatedname"`
	Lat          float64 `json:"lat" db:"lat"`
	Lon          float64 `json:"lon" db:"lon"`
	Filename     string  `json:"filename" db:"filename"`
	Requestedby  string  `json:"requestedby" db:"requestedby"`
	Rejectedby   string  `json:"rejectedby" db:"rejectedby"`
	Approvedby   string  `json:"approvedby" db:"approvedby"`
	Updatedby    string  `json:"updatedby" db:"updatedby"`
	Updatedat    string  `json:"updatedat" db:"updatedat"`
	Createdat    string  `json:"createdat" db:"createdat"`
	Approvedat   string  `json:"approvedat" db:"approvedat"`
	Currentdate  string  `json:"currentdate" db:"currentdate"`
	Countryid    int     `json:"countryid" db:"countryid"`
	Referenceid  string  `json:"referenceid" db:"referenceid"`
	Status       string  `json:"status" db:"status"`
	Uploadstatus string  `json:"uploadstatus" db:"uploadstatus"`
}

func (p *Pg) DumpIntoPostgres(data []Data) error {

	var schemaList []Schema = make([]Schema, 0)
	conn := p.GetPgConnection()

	query := `insert into address (id,
		                          name,
								  originalname,
								  lat,
								  lon,
								  level_0,
								  level_1,
								  level_2,
								  level_3,
								  level_4,
								  level_5,
								  level_0_bn,
								  level_1_bn,
								  level_2_bn,
								  level_3_bn,
								  level_4_bn,
								  level_5_bn,
								  area,
								  area_bn,
								  popularity,
								  status,
								  uploadstatus
								  ) values ( :id,
									:name,
									:originalname,
									:lat,
									:lon,
									:level_0,
									:level_1,
									:level_2,
									:level_3,
									:level_4,
									:level_5,
									:level_0_bn,
									:level_1_bn,
									:level_2_bn,
									:level_3_bn,
									:level_4_bn,
									:level_5_bn,
									:area,
									:area_bn,
									:popularity,
									:status,
									:uploadstatus)`

	for i := 0; i < len(data); i++ {
		schema := Schema{
			Id:           data[i].ID,
			Name:         data[i].Source.Name,
			Originalname: data[i].Source.Name,
			Lat:          data[i].Source.Lat,
			Lon:          data[i].Source.Lon,
			Level_0:      data[i].Source.Level0,
			Level_1:      data[i].Source.Level1,
			Level_2:      data[i].Source.Level2,
			Level_3:      data[i].Source.Level3,
			Level_4:      data[i].Source.Level4,
			Level_5:      data[i].Source.Level5,
			Level_0_bn:   data[i].Source.Level0Bn,
			Level_1_bn:   data[i].Source.Level1Bn,
			Level_2_bn:   data[i].Source.Level2Bn,
			Level_3_bn:   data[i].Source.Level3Bn,
			Level_4_bn:   data[i].Source.Level4Bn,
			Level_5_bn:   data[i].Source.Level5Bn,
			Area:         data[i].Source.Area,
			Area_bn:      data[i].Source.AreaBn,
			Popularity:   data[i].Source.Popularity,
			Status:       "APPROVED",
			Uploadstatus: "UPLOADED",
		}
		schemaList = append(schemaList, schema)
	}

	chunkedData := DivideIntoChunk(schemaList)

	fmt.Printf("Total batches : %v\n", len(chunkedData))

	for i := 0; i < len(chunkedData); i++ {
		_, err := conn.NamedExec(query, chunkedData[i])
		if err != nil {
			return err
		}
		fmt.Printf("Inserted batch : %d\r", i)
	}

	return nil
}

type ChunkedData [][]Schema

func DivideIntoChunk(data []Schema) ChunkedData {
	limit := 500
	chunkData := ChunkedData{}

	Size := float64(len(data)) / float64(limit)
	roundedSize := int(math.Round(float64(Size)))

	j := 0
	for i := 0; i < roundedSize; i++ {
		if len(data)-j >= limit {
			tempData := data[j : limit+j]
			chunkData = append(chunkData, tempData)
			j += limit

		} else if len(data)-j < limit {
			chunkData = append(chunkData, data[j:])
		}
	}

	return chunkData

}
