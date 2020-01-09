package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/urfave/cli"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

var (
	namespace             string
	deploymentName        string
	containerName         string
	kubeConfig            string
	leaseTableNamePattern string
	region                string
	dryRun                bool
	shardDynamoKey        = "ShardID"
	clientset             *kubernetes.Clientset
	dynamo                *dynamodb.DynamoDB

//	namespace = "antiabuse"
)

func main() {
	app := cli.NewApp()
	app.Usage = "restarter queries kubernetes for the given deployment, parses its config for an env var named *_LEASE_TABLE, scales the deployment to 0, clears the DynamoDB lease table, and scales the deployment back up. It's intended to be used in scaling or new deployment scenarios when lease stealing isn't configured for the consumers."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "region",
			EnvVar:      "AWS_REGION",
			Value:       "local",
			Usage:       "the region the table would be located in",
			Destination: &region,
		},
		cli.StringFlag{
			Name:   "kubeconfig",
			EnvVar: "KUBE_CONFIG",
			Usage:  "the location of the kube config file",
			Value:  defaultConfig(),
			//apiv1.NamespaceDefault
			Destination: &kubeConfig,
		},
		cli.StringFlag{
			Name:   "namespace, ns",
			EnvVar: "NAMESPACE",
			Usage:  "the k8s namespace to search, defaults to \"\"",
			Value:  "antiabuse",
			//apiv1.NamespaceDefault
			Destination: &namespace,
		},
		cli.StringFlag{
			Name:        "deployment, d",
			EnvVar:      "DEPLOYMENT",
			Usage:       "the deployment to restart and clear the lease tables for",
			Destination: &deploymentName,
		},
		cli.StringFlag{
			Name:        "lease-table-pattern, l",
			EnvVar:      "LEASE_TABLE_PATTERN",
			Usage:       "the string blob to search the deployment env for to identify the necessary lease table",
			Value:       "LEASE_TABLE",
			Destination: &leaseTableNamePattern,
		},
		cli.StringFlag{
			Name:        "container, c",
			EnvVar:      "CONTAINER",
			Destination: &containerName,
		},
		cli.BoolFlag{
			Name:        "dryrun",
			EnvVar:      "DRY_RUN",
			Usage:       "whether to run the restarter in dry run mode, so that it only logs the actions it would take",
			Destination: &dryRun,
		},
	}
	app.Before = setupClient
	app.Action = restartDeployment

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}

func defaultConfig() string {
	if home := homeDir(); home != "" {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

func setupClient(ctx *cli.Context) error {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	dynamo = dynamodb.New(sess)

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		return fmt.Errorf("failed to build kube config: %w", err)
	}

	// create the clientset
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to instantiatee kube client: %w", err)
	}
	return nil
}

func restartDeployment(ctx *cli.Context) error {

	dInfo, err := getDeploymentInfo()
	if err != nil {
		return err
	}

	// scale deployment down
	if err := scaleDeployment(dInfo.deployment, 0); err != nil {
		return err
	}

	leaseTable, err := parseLeaseTable(dInfo)
	if err != nil {
		return err
	}

	leases, err := getLeases(leaseTable)
	if err != nil {
		return err
	}

	if err := deleteLeases(leaseTable, leases); err != nil {
		return err
	}

	// get latest info
	newInfo, err := getDeploymentInfo()
	if err != nil {
		return err
	}

	// scale deployment up
	if err := scaleDeployment(newInfo.deployment, dInfo.oldReplicas); err != nil {
		return err
	}

	return nil
}

type info struct {
	deployment  *v1.Deployment
	oldReplicas int32
}

func scaleDeployment(deployment *v1.Deployment, replicas int32) error {
	fmt.Printf("updating deployment %s\n", deploymentName)
	deploymentsClient := clientset.AppsV1().Deployments(namespace)
	if dryRun {
		fmt.Printf("deployment not updated, as it is a dry run\n")
	} else {
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			deployment.Spec.Replicas = &replicas
			_, updateErr := deploymentsClient.Update(deployment)
			return updateErr
		})
		if retryErr != nil {
			return fmt.Errorf("failed to update deployment: %w", retryErr)
		}
	}
	fmt.Println("Updated deployment...")
	return nil
}

func getDeploymentInfo() (*info, error) {
	fmt.Printf("namespace: %s deployment: %s\n", namespace, deploymentName)
	deploymentsClient := clientset.AppsV1().Deployments(namespace)
	deployment, err := deploymentsClient.Get(deploymentName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("deployment %s in namespace %s not found", deploymentName, namespace)
		} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
			return nil, fmt.Errorf("error getting pod %s in namespace %s: %w",
				deploymentName, namespace, statusError)
		}
		return nil, fmt.Errorf("failed to get deployment: %w", err)
	}
	//prettyPrint(deployment)

	// get the number of replicas
	return &info{
		oldReplicas: *deployment.Spec.Replicas,
		deployment:  deployment,
	}, nil
}

func parseLeaseTable(dInfo *info) (string, error) {
	deployment := dInfo.deployment
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		return "", fmt.Errorf("deployment %s has no containers definitions to parse the lease table name from", deploymentName)
	}

	var idx int
	for i, container := range deployment.Spec.Template.Spec.Containers {
		if containerName != "" && container.Name == containerName {
			idx = i
		}
	}

	envVars := deployment.Spec.Template.Spec.Containers[idx].Env
	var leaseTable string
	for _, envVar := range envVars {
		if strings.Contains(envVar.Name, leaseTableNamePattern) {
			leaseTable = envVar.Value
		}
	}

	if leaseTable == "" {
		return "", fmt.Errorf("found no env var matching the lease table name pattern: %s", leaseTableNamePattern)
	}

	return leaseTable, nil
}

func getLeases(leaseTable string) ([]string, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(leaseTable),
	}

	output, err := dynamo.Scan(input)
	if err != nil {
		return nil, fmt.Errorf("failed to scan lease table %s: %w", leaseTable, err)
	}

	res := make([]string, 0, len(output.Items))
	for i := range output.Items {
		lease, ok := output.Items[i][shardDynamoKey]
		if !ok {
			continue
		}
		res = append(res, *lease.S)
	}
	return res, nil
}

func deleteLeases(leaseTable string, leases []string) error {
	for _, lease := range leases {
		if dryRun {
			fmt.Printf("would have deleted lease %s from lease table %s\n", lease, leaseTable)
			continue
		}
		input := &dynamodb.DeleteItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				shardDynamoKey: {
					S: aws.String(lease),
				},
			},
			TableName: aws.String(leaseTable),
		}

		_, err := dynamo.DeleteItem(input)
		if err != nil {
			return fmt.Errorf("failed to delete lease %s from lease table %s: %w", lease, leaseTable, err)
		}
	}
	return nil
}

func prettyPrint(i interface{}) {
	b, err := json.MarshalIndent(i, "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", string(b))
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
