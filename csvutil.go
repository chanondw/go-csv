package csvutil

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
)

//	 Tags to read on struct will be in the form of `col:"1"` being "1" is the column number in the csv file
//	 eg.
//		type Test struct {
//		    Field1 string `col:"column name"`
//		}
func ReadToStruct[T any](filename string) ([]T, error) {
	records, err := readFileToArr(filename)
	if err != nil {
		return nil, fmt.Errorf("read file error %s", err)
	}

	str := []T{}
	convToInterface, err := readColumnDefCreateStruct[T](records[0])
	if err != nil {
		return nil, err
	}

	for i, r := range records {
		if i == 0 {
			continue
		}
		if elem, err := convToInterface(r); err != nil {
			return nil, err
		} else {
			str = append(str, *elem)
		}
	}

	return str, nil
}

// Write to CSV using tag from stuct
// eg.
//
//	type Test struct {
//	   Field1 string `col:"column name"`
//	}
//
// will become
// | column name  |
// | field1 value |
func WriteFromStruct[T any](filename string, in []T) error {
	out := [][]string{}
	header, err := getStructTagForHeader[T]()
	if err != nil {
		return err
	}
	headRow := make([]string, len(header))
	for i, v := range header {
		headRow[i] = v
	}

	out = append(out, headRow)
	for _, r := range in {
		row := make([]string, len(headRow))
		str := reflect.ValueOf(r)

		for i := range header {
			field := str.Field(i)
			switch field.Kind() {
			case reflect.Invalid:
				err := fmt.Errorf("field type not supported %s", field.Kind())
				return err
			case reflect.Bool:
				row[i] = strconv.FormatBool(field.Bool())
				break
			case reflect.Int32:
				fallthrough
			case reflect.Int8:
				fallthrough
			case reflect.Int16:
				fallthrough
			case reflect.Int64:
				fallthrough
			case reflect.Int:
				row[i] = strconv.FormatInt(field.Int(), 10)
				break
			case reflect.Float32:
				row[i] = strconv.FormatFloat(field.Float(), 'f', 0, 32)
				break
			case reflect.Float64:
				row[i] = strconv.FormatFloat(field.Float(), 'f', 0, 64)
				break
			case reflect.String:
				row[i] = field.String()
				break
			default:
				return fmt.Errorf("unsupport type %s", field.Kind())

			}
		}

		out = append(out, row)
	}

	wf, err := os.Create(filename)
	if err != nil {
		fmt.Println("Unable to write file", err)
		return err
	}

	csvWriter := csv.NewWriter(wf)
	if err = csvWriter.WriteAll(out); err != nil {
		fmt.Println("write error", err)
		return err
	}

	return nil
}

func readFileToArr(filename string) (rows [][]string, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to read file %s", err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("unable to parse file as CSV %s", err)
	}

	return records, nil
}

func readColumnDefCreateStruct[T any](colHeader []string) (func(row []string) (*T, error), error) {
	elem := reflect.TypeOf(new(T)).Elem()
	if elem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is not struct", elem)
	}
	colDef, outErr := getStructTags(elem, colHeader)
	if outErr != nil {
		return nil, fmt.Errorf("error during reading column tag %s", outErr)
	}

	return func(row []string) (*T, error) {
		t := new(T)
		str := reflect.ValueOf(t).Elem()
		for k, v := range colDef {
			switch field := str.FieldByName(k); field.Kind() {
			case reflect.Invalid:
				err := fmt.Errorf("field type not supported %s", k)
				return nil, err
			case reflect.Bool:
				out, err := strconv.ParseBool(row[v])
				if err != nil {
					err = fmt.Errorf("field bool %s invalid: %s", k, err)
					return nil, err
				}
				field.SetBool(out)
				break
			case reflect.Int32:
				fallthrough
			case reflect.Int8:
				fallthrough
			case reflect.Int16:
				fallthrough
			case reflect.Int64:
				fallthrough
			case reflect.Int:
				out, err := strconv.ParseInt(row[v], 10, 32)
				if err != nil {
					err = fmt.Errorf("field int %s invalid: %s", k, err)
					return nil, err
				}
				field.SetInt(out)
				break
			case reflect.Float32:
				out, err := strconv.ParseFloat(row[v], 32)
				if err != nil {
					err = fmt.Errorf("field bool %s invalid: %s", k, err)
					return nil, err
				}
				field.SetFloat(out)
				break
			case reflect.Float64:
				out, err := strconv.ParseFloat(row[v], 64)
				if err != nil {
					err = fmt.Errorf("field bool %s invalid: %s", k, err)
					return nil, err
				}
				field.SetFloat(out)
				break
			case reflect.String:
				field.SetString(row[v])
				break
			default:
				return nil, fmt.Errorf("unsupport type %s", k)
			}
		}

		res := str.Interface().(T)
		return &res, nil
	}, nil
}

func getStructTags(T reflect.Type, colHeader []string) (map[string]int, error) {
	if T.Kind() != reflect.Struct && T.Kind() != reflect.Interface {
		return nil, fmt.Errorf("%s is not a struct", T)
	}

	colNum := map[string]int{}
	for i, v := range colHeader {
		colNum[v] = i
	}

	m := make(map[string]int)
	for i := 0; i < T.NumField(); i++ {
		fld := T.Field(i)
		if col := fld.Tag.Get("col"); col != "" {
			if n, ok := colNum[col]; !ok {
				return nil, fmt.Errorf("column %s does not exist", col)
			} else {
				m[fld.Name] = n
			}
		}
	}
	return m, nil
}

func getStructTagForHeader[T any]() (map[int]string, error) {
	elem := reflect.TypeOf(new(T)).Elem()
	if elem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is not struct", elem)
	}

	if elem.Kind() != reflect.Struct && elem.Kind() != reflect.Interface {
		return nil, fmt.Errorf("%s is not a struct", elem)
	}
	out := map[int]string{}
	for i := 0; i < elem.NumField(); i++ {
		fld := elem.Field(i)
		if col := fld.Tag.Get("col"); col != "" {
			out[i] = col
		}
	}
	return out, nil
}
