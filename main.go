/*
2018/09/09 20:33:22 [GetCustomerBatchFn] batch size: 3
2018/09/09 20:33:22 [GetCustomerAffiliationsBatchFn] batch size: 3
2018/09/09 20:33:22 [GetGroupBatchFn] batch size: 4
2018/09/09 20:33:22 [GraphQL result] total customers: 3
2018/09/09 20:33:22 [GraphQL result]:
{"data":{"CustomerVisit":{"items":[{"customer":{"affiliations":{"items":[{"group":{"id":1,"name":"first group"}},{"group":{"id":4,"name":"fourth group"}}]},"first_name":"first customer","id":1,"last_name":"first customer last name"}},{"customer":{"affiliations":{"items":[{"group":{"id":2,"name":"second group"}},{"group":{"id":4,"name":"fourth group"}}]},"first_name":"second customer","id":2,"last_name":"second customer last name"}},{"customer":{"affiliations":{"items":[{"group":{"id":3,"name":"third group"}},{"group":{"id":4,"name":"fourth group"}}]},"first_name":"third customer","id":3,"last_name":"third customer last name"}}]}}}
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"github.com/graph-gophers/dataloader"
	"github.com/graphql-go/graphql"
)

type ResolverKey struct {
	Key    string
	Client *Client
}

func NewResolverKey(key string, client *Client) *ResolverKey {
	return &ResolverKey{
		Key:    key,
		Client: client,
	}
}

func (rk *ResolverKey) String() string {
	return rk.Key
}

func (rk *ResolverKey) Raw() interface{} {
	return rk.Key
}

func (rk *ResolverKey) client() *Client {
	return rk.Client
}

type Client struct {
	db *gorm.DB
}

func (c *Client) ListCustomers(customerIDs []uint) ([]Customer, error) {
	var customers []Customer
	if err := c.db.Where("id IN (?)", customerIDs).Find(&customers).Error; err != nil {
		return customers, err
	}
	return customers, nil
}

func (c *Client) ListAffiliations(customerIDs []uint) ([]*Affiliation, error) {
	var affiliations []*Affiliation
	if err := c.db.Where("customer_id IN (?)", customerIDs).Find(
		&affiliations).Error; err != nil {
		log.Println(err)
		return affiliations, err
	}
	return affiliations, nil
}

func (c *Client) ListGroups(groupIDs []uint) ([]Group, error) {
	var groups []Group
	if err := c.db.Where("id IN (?)", groupIDs).Find(&groups).Error; err != nil {
		return groups, err
	}
	return groups, nil
}

func NewClient(db *gorm.DB) *Client {
	return &Client{
		db: db,
	}
}

type Customer struct {
	ID           uint   `graphql:"id"`
	FirstName    string `graphql:"first_name"`
	LastName     string `graphql:"last_name"`
	Affiliations Affiliations
}

type Group struct {
	ID   uint
	Name string
}

type Affiliation struct {
	ID         uint `graphql:"id"`
	CustomerID uint `graphql:"customer_id"`
	GroupID    uint `graphql:"group_id"`
}

type Affiliations struct {
	Items []*Affiliation
}

type CustomerVisitItem struct {
	Customer Customer
}

type CustomerVisit struct {
	Items []CustomerVisitItem
}

var GroupType = graphql.NewObject(graphql.ObjectConfig{
	Name: "GroupType",
	Fields: graphql.Fields{
		"id":   &graphql.Field{Type: graphql.Int},
		"name": &graphql.Field{Type: graphql.String},
	},
})

var AffiliationItemType = graphql.NewObject(graphql.ObjectConfig{
	Name: "AffiliationItemType",
	Fields: graphql.Fields{
		"group": &graphql.Field{
			Type:    GroupType,
			Resolve: GroupResolver,
		},
	},
})

var AffiliationType = graphql.NewObject(graphql.ObjectConfig{
	Name: "AffiliationType",
	Fields: graphql.Fields{
		"items": &graphql.Field{
			Type: graphql.NewList(AffiliationItemType),
		},
	},
})

var CustomerType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Customer",
	Fields: graphql.Fields{
		"id":         &graphql.Field{Type: graphql.Int},
		"first_name": &graphql.Field{Type: graphql.String},
		"last_name":  &graphql.Field{Type: graphql.String},
		"affiliations": &graphql.Field{
			Type:    AffiliationType,
			Resolve: CustomerAffiliationsResolver,
		},
	},
})

var CustomerVisitItemType = graphql.NewObject(graphql.ObjectConfig{
	Name: "CustomerVisitItem",
	Fields: graphql.Fields{
		"customer": &graphql.Field{
			Type: CustomerType,
		},
	},
})

var CustomerVisitType = graphql.NewObject(graphql.ObjectConfig{
	Name: "CustomerVisit",
	Fields: graphql.Fields{
		"items": &graphql.Field{
			Type: graphql.NewList(CustomerVisitItemType),
		},
	},
})

var QueryType = graphql.NewObject(graphql.ObjectConfig{
	Name: "Query",
	Fields: graphql.Fields{
		"CustomerVisit": &graphql.Field{
			Type:    CustomerVisitType,
			Resolve: CustomerVisitResolver,
		},
	},
})

func main() {
	dsn := "host=localhost user=postgres_user password=password dbname=pr388 port=25432 sslmode=disable TimeZone=Asia/Shanghai"
	db, err := gorm.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.AutoMigrate(&Customer{})
	db.AutoMigrate(&Group{})
	db.AutoMigrate(&Affiliation{})
	db.LogMode(false)
	if err := createTestData(db); err != nil {
		log.Fatal(err)
	}
	db = db.Debug()
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: QueryType,
	})
	if err != nil {
		log.Fatal(err)
	}
	query := `
		query ListCustomerVisits {
			CustomerVisit {
				items {
					customer {
						id
						first_name
						last_name
						affiliations {
							items {
								group {
									id
									name
								}
							}
						}
					}
				}
			}
		}
	`

	var loaders = make(map[string]*dataloader.Loader, 3)
	loaders["GetCustomer"] = dataloader.NewBatchedLoader(GetCustomerBatchFn)
	loaders["GetCustomerAffiliations"] = dataloader.NewBatchedLoader(
		GetCustomerAffiliationsBatchFn)
	loaders["GetGroup"] = dataloader.NewBatchedLoader(GetGroupBatchFn)

	rootObject := map[string]interface{}{
		"client": NewClient(db),
	}

	result := graphql.Do(graphql.Params{
		Context:       context.WithValue(context.Background(), "loaders", loaders),
		RequestString: query,
		RootObject:    rootObject,
		Schema:        schema,
	})
	b, err := json.Marshal(result)
	if err != nil {
		log.Fatal(err)
	}

	d := struct {
		Data struct {
			CustomerVisit CustomerVisit
		}
	}{}

	if err := json.Unmarshal(b, &d); err != nil {
		log.Fatal(err)
	}
	log.Printf("[GraphQL result] total customers: %d", len(d.Data.CustomerVisit.Items))
	log.Printf("[GraphQL result]: \n%s\n", b)
}

func CustomerVisitResolver(p graphql.ResolveParams) (interface{}, error) {
	var customerVisit CustomerVisit
	var customerVisitItems []CustomerVisitItem
	var loaders = p.Context.Value("loaders").(map[string]*dataloader.Loader)
	var customerLoader = loaders["GetCustomer"]
	var customerIDs = []int{1, 2, 3}
	var rootValue = p.Info.RootValue.(map[string]interface{})
	var client = rootValue["client"].(*Client)

	var keys dataloader.Keys
	for _, id := range customerIDs {
		resolverKey := NewResolverKey(fmt.Sprintf("%d", id), client)
		keys = append(keys, resolverKey)
	}

	handleErrors := func(errors []error) error {
		var errs []string
		for _, e := range errors {
			errs = append(errs, e.Error())
		}
		return fmt.Errorf(strings.Join(errs, "\n"))
	}
	customerResults, errs := customerLoader.LoadMany(p.Context, keys)()
	if len(errs) > 0 {
		return nil, handleErrors(errs)
	}

	for _, cr := range customerResults {
		customer := cr.(*Customer)
		cvi := CustomerVisitItem{Customer: *customer}
		customerVisitItems = append(customerVisitItems, cvi)
	}

	customerVisit = CustomerVisit{Items: customerVisitItems}
	return &customerVisit, nil
}

func CustomerAffiliationsResolver(p graphql.ResolveParams) (interface{}, error) {
	var customer = p.Source.(Customer)
	var loaders = p.Context.Value("loaders").(map[string]*dataloader.Loader)
	var affiliationsLoader = loaders["GetCustomerAffiliations"]
	var rootValue = p.Info.RootValue.(map[string]interface{})
	var client = rootValue["client"].(*Client)
	var key = NewResolverKey(fmt.Sprintf("%d", customer.ID), client)
	var affiliations []*Affiliation

	type result struct {
		data interface{}
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		loaderResult, err := affiliationsLoader.Load(p.Context, key)()
		if err != nil {
			ch <- result{err: err}
		}
		affiliations = loaderResult.([]*Affiliation)
		ch <- result{
			data: &Affiliations{Items: affiliations},
			err:  nil,
		}
		close(ch)
	}()

	return func() (interface{}, error) {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		return r.data, nil
	}, nil
}

func GroupResolver(p graphql.ResolveParams) (interface{}, error) {
	var loaders = p.Context.Value("loaders").(map[string]*dataloader.Loader)
	var rootValue = p.Info.RootValue.(map[string]interface{})
	var client = rootValue["client"].(*Client)
	var key = NewResolverKey(fmt.Sprintf("%d", p.Source.(*Affiliation).GroupID), client)
	thunk := loaders["GetGroup"].Load(p.Context, key)
	return func() (interface{}, error) {
		group, err := thunk()
		if err != nil {
			return nil, err
		}
		return group.(Group), nil
	}, nil
}

func GetGroupBatchFn(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
	handleError := func(err error) []*dataloader.Result {
		var results []*dataloader.Result
		var result dataloader.Result
		result.Error = err
		results = append(results, &result)
		return results
	}

	var groupIDs []uint
	for _, key := range keys {
		id, err := strconv.ParseUint(key.String(), 10, 32)
		if err != nil {
			return handleError(err)
		}
		groupIDs = append(groupIDs, uint(id))
	}

	var results []*dataloader.Result
	groups, err := keys[0].(*ResolverKey).client().ListGroups(groupIDs)
	if err != nil {
		return handleError(err)
	}

	var groupsMap = make(map[uint]Group, len(groups))
	for _, group := range groups {
		groupsMap[group.ID] = group
	}

	for _, groupID := range groupIDs {
		group, _ := groupsMap[groupID]
		result := dataloader.Result{
			Data: group,
		}
		results = append(results, &result)
	}

	log.Printf("[GetGroupBatchFn] batch size: %d", len(results))
	return results
}

func GetCustomerBatchFn(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
	handleError := func(err error) []*dataloader.Result {
		var results []*dataloader.Result
		var result dataloader.Result
		result.Error = err
		results = append(results, &result)
		return results
	}
	var customerIds []uint
	for _, key := range keys {
		id, err := strconv.ParseUint(key.String(), 10, 32)
		if err != nil {
			return handleError(err)
		}
		customerIds = append(customerIds, uint(id))
	}
	customers, err := keys[0].(*ResolverKey).client().ListCustomers(customerIds)
	if err != nil {
		return handleError(err)
	}
	var customersMap = make(map[uint]*Customer, len(customerIds))
	for _, c := range customers {
		customer := c
		customersMap[customer.ID] = &customer
	}
	var results []*dataloader.Result
	for _, customerID := range customerIds {
		customer, ok := customersMap[customerID]
		if !ok {
			return nil
		}
		r := dataloader.Result{
			Data:  customer,
			Error: nil,
		}
		results = append(results, &r)
	}
	log.Printf("[GetCustomerBatchFn] batch size: %d", len(results))
	return results
}

func GetCustomerAffiliationsBatchFn(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
	handleError := func(err error) []*dataloader.Result {
		var results []*dataloader.Result
		var result dataloader.Result
		result.Error = err
		results = append(results, &result)
		return results
	}

	var results []*dataloader.Result
	var customerIDs []uint
	for _, key := range keys {
		id, err := strconv.ParseUint(key.String(), 10, 32)
		if err != nil {
			return handleError(err)
		}
		customerIDs = append(customerIDs, uint(id))
	}

	affiliations, err := keys[0].(*ResolverKey).client().ListAffiliations(customerIDs)
	if err != nil {
		return handleError(err)
	}

	var affiliationsMap = make(map[uint][]*Affiliation)
	for _, a := range affiliations {
		affiliation := a
		if _, found := affiliationsMap[affiliation.CustomerID]; !found {
			affiliationsMap[affiliation.CustomerID] = []*Affiliation{
				affiliation,
			}
		} else {
			affiliationsMap[affiliation.CustomerID] = append(
				affiliationsMap[affiliation.CustomerID], affiliation)
		}
	}

	for _, customerID := range customerIDs {
		affiliations, ok := affiliationsMap[customerID]
		if !ok {
			return nil
		}
		r := dataloader.Result{
			Data:  affiliations,
			Error: nil,
		}
		results = append(results, &r)
	}

	log.Printf("[GetCustomerAffiliationsBatchFn] batch size: %d", len(results))
	return results
}

func createTestData(db *gorm.DB) error {
	var customersCount int
	if err := db.Table("customers").Count(&customersCount).Error; err != nil {
		log.Fatal(err)
	}
	if customersCount == 0 {
		q := `
		INSERT INTO customers (first_name, last_name) VALUES
		    ('first customer', 'first customer last name'),
		    ('second customer', 'second customer last name'),
		    ('third customer', 'third customer last name');
		`
		if err := db.Exec(q).Error; err != nil {
			return err
		}
	}

	var groupsCount int
	if err := db.Table("groups").Count(&groupsCount).Error; err != nil {
		return err
	}
	if groupsCount == 0 {
		q := `
		INSERT INTO groups (name) VALUES
		    ('first group'),
		    ('second group'),
		    ('third group'),
		    ('fourth group');
		`
		if err := db.Exec(q).Error; err != nil {
			return err
		}
	}

	var affiliationsCount int
	if err := db.Table("affiliations").Count(&affiliationsCount).Error; err != nil {
		return err
	}
	if affiliationsCount == 0 {
		q := `
		INSERT INTO affiliations (customer_id, group_id) VALUES
		    (1, 1),
		    (1, 4),
		    (2, 2),
		    (2, 4),
		    (3, 3),
		    (3, 4);
		`
		if err := db.Exec(q).Error; err != nil {
			return err
		}
	}
	return nil
}
