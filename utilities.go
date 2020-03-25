package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/andy-cywang/aws-face-rekognition-pb/employeepb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"image/jpeg"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rekognition"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	ot "github.com/opentracing/opentracing-go"
)

var (
	emailRegexp = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
)

func validateEmailFormat(email string) bool {
	return emailRegexp.MatchString(email)
}

func CallEmployeeDBServiceToCreateEmp(ctx context.Context, tempEmp employee, address string) (string, error) {
	conn, err := createGRPCConn(ctx, address)
	if err != nil {
		return "", status.Errorf(codes.Internal, "Cannot connect to database when creating employee: %v", err.Error())
	}
	defer conn.Close()

	client := employeepb.NewEmployeeDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	req := employeepb.CreateEmployeeRequest{
		Employee: &employeepb.Employee{
			EmpID:              tempEmp.empID,
			EmpFirst:           tempEmp.empFirst,
			EmpLast:            tempEmp.empLast,
			EmpEmail:           tempEmp.empEmail,
			EmpCardID:          tempEmp.empCardID,
			EmpExternalImageID: tempEmp.empExternalImageID,
		},
	}
	emp, err := client.CreateEmployee(ctx, &req)
	if err != nil {
		return "", status.Errorf(codes.Internal, "Error when creating employee: %v", err.Error())
	}

	return emp.GetMessage(), nil
}

func CallEmployeeDBServiceToSearchFace(ctx context.Context, empEmail string, address string) (*employeepb.Employee, error) {
	conn, err := createGRPCConn(ctx, address)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot connect to database when searching employee: %v", err.Error())
	}
	defer conn.Close()

	client := employeepb.NewEmployeeDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	req := employeepb.SearchEmployeeByFaceRequest{
		EmpEmail: empEmail,
	}

	result, err := client.SearchEmployeeByFace(ctx, &req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error when searching employee: %v", err.Error())
	}

	return result.GetEmployee(), nil
}

func CallEmployeeDBServiceToSearchCard(ctx context.Context, empCardID string, address string) (*employeepb.Employee, error) {
	conn, err := createGRPCConn(ctx, address)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot connect to database when searching employee: %v", err.Error())
	}
	defer conn.Close()

	client := employeepb.NewEmployeeDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	req := employeepb.SearchEmployeeByCardRequest{
		EmpCardID: empCardID,
	}

	result, err := client.SearchEmployeeByCard(ctx, &req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error when searching employee: %v", err.Error())
	}

	return result.GetEmployee(), nil
}

func CallEmployeeDBServiceToSignInEmp(ctx context.Context, empEmail string, empCardID string, address string) error {

	conn, err := createGRPCConn(ctx, address)
	if err != nil {
		return status.Errorf(codes.Internal, "Cannot connect to database when sign in employee: %v", err.Error())
	}
	defer conn.Close()

	client := employeepb.NewEmployeeDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	req := employeepb.SignInEmployeeRequest{
		EmpEmail:  empEmail,
		EmpCardID: empCardID,
	}

	_, err = client.SignInEmployee(ctx, &req)
	if err != nil {
		return status.Errorf(codes.Internal, "Error when sign in employee: %v", err.Error())
	}

	return nil
}

func CallEmployeeDBServiceToGetAllEmp(ctx context.Context, address string) ([]*employeepb.Employee, error) {

	conn, err := createGRPCConn(ctx, address)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot connect to database when getting all employees: %v", err.Error())
	}
	defer conn.Close()

	client := employeepb.NewEmployeeDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	result, err := client.GetAllEmployees(ctx, new(empty.Empty))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error when getting all employees: %v", err.Error())
	}

	return result.GetEmployee(), nil
}

func createGRPCConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithStreamInterceptor(
		grpc_opentracing.StreamClientInterceptor(
			grpc_opentracing.WithTracer(ot.GlobalTracer()))))
	opts = append(opts, grpc.WithUnaryInterceptor(
		grpc_opentracing.UnaryClientInterceptor(
			grpc_opentracing.WithTracer(ot.GlobalTracer()))))
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to application addr: ", err)
		return nil, err
	}
	return conn, nil
}

func getImageBytesFromFile(filename string) []byte {
	fname := filename
	infile, err := os.Open(fname)
	if err != nil {
		log.Fatalln("Error when openning file", err.Error())
	}
	defer infile.Close()

	img, err := jpeg.Decode(infile)
	if err != nil {
		log.Fatalln("Error when decoding file", err.Error())
	}

	buff := new(bytes.Buffer)
	err = jpeg.Encode(buff, img, nil)
	if err != nil {
		fmt.Println("Failed to create buffer", err)
	}
	imageBytes := buff.Bytes()

	return imageBytes
}

func getImageBytesFromBase64String(imageBase64 string) []byte {
	imageBytes, err := base64.StdEncoding.DecodeString(imageBase64)
	if err != nil {
		log.Println("Error when decode base64 string")
	}

	return imageBytes
}

func awsRekognitionErrorHandler(err error) (errorCode string, errorMsg string) {

	var errCode, errMsg string

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case rekognition.ErrCodeInvalidS3ObjectException:
			errCode, errMsg = rekognition.ErrCodeInvalidS3ObjectException, aerr.Error()
		case rekognition.ErrCodeInvalidParameterException:
			errCode, errMsg = rekognition.ErrCodeInvalidParameterException, aerr.Error()
		case rekognition.ErrCodeImageTooLargeException:
			errCode, errMsg = rekognition.ErrCodeImageTooLargeException, aerr.Error()
		case rekognition.ErrCodeAccessDeniedException:
			errCode, errMsg = rekognition.ErrCodeAccessDeniedException, aerr.Error()
		case rekognition.ErrCodeInternalServerError:
			errCode, errMsg = rekognition.ErrCodeInternalServerError, aerr.Error()
		case rekognition.ErrCodeThrottlingException:
			errCode, errMsg = rekognition.ErrCodeThrottlingException, aerr.Error()
		case rekognition.ErrCodeProvisionedThroughputExceededException:
			errCode, errMsg = rekognition.ErrCodeProvisionedThroughputExceededException, aerr.Error()
		case rekognition.ErrCodeResourceNotFoundException:
			errCode, errMsg = rekognition.ErrCodeResourceNotFoundException, aerr.Error()
		case rekognition.ErrCodeInvalidImageFormatException:
			errCode, errMsg = rekognition.ErrCodeInvalidImageFormatException, aerr.Error()
		default:
			errCode, errMsg = "Unkonwn aws error code", aerr.Error()
		}
	} else {
		errCode, errMsg = "Unkonwn error", err.Error()
	}

	return errCode, errMsg
}
