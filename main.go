package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rekognition"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"

	"github.com/andy-cywang/aws-face-rekognition-pb/recognitionpb"
	"google.golang.org/grpc"
)

const (
	port                      = ":50051"
	enrollConfidenceThreshold = 70.00
	searchConfidenceThreshold = 90.00
	employeedbAddress         = "localhost:50052"
)

type recognitionServiceServer struct {
}

type employee struct {
	empID              string
	empCardID          string
	empFirst           string
	empLast            string
	empEmail           string
	empExternalImageID string
}

func (s *recognitionServiceServer) CreateEmployee(ctx context.Context, req *recognitionpb.CreateEmployeeRequest) (*recognitionpb.CreateEmployeeResponse, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Recognition-Create: Error when create session %v", err)
	}
	svc := rekognition.New(sess)

	// get employee data from the request
	emp := req.GetEmployee()
	if emp == nil || emp.GetEmpEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "Recognition-Create: Error when receiving data")
	} else if !validateEmailFormat(emp.GetEmpEmail()) {
		return nil, status.Error(codes.InvalidArgument, "Recognition-Create: Invalid email format")
	}

	// check if employee already exists in the database
	existingEmployee, err := CallEmployeeDBServiceToSearchFace(ctx, emp.GetEmpEmail(), employeedbAddress)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Recognition-Create: Error when connecting employee db: %v", err)
	}

	if existingEmployee.GetEmpEmail() != "" {
		return nil, status.Error(codes.AlreadyExists, "Recognition-Create: Employee already exists")
	}

	// empExternalImageID is used for reference when searching employee in the database. AWS Rekognition cannot use
	// string with "@" as image external ID, thus we need to replace "@" with "AT" for this purpose
	empExternalImageID := strings.ReplaceAll(strings.ToLower(emp.GetEmpEmail()), "@", "AT")

	// TODO: add face to aws rekognition collection
	//imageBytes := getImageBytesFromFile("/home/dyson.global.corp/chenwang/Desktop/Tom-Hardy.jpg")
	//imageBytes := getImageBytesFromBase64String(emp.GetFace().EmpFace)
	input := &rekognition.IndexFacesInput{
		CollectionId:        aws.String("testPhotos"),
		ExternalImageId:     aws.String(empExternalImageID),
		QualityFilter:       aws.String("AUTO"),
		DetectionAttributes: []*string{aws.String("ALL")},
		Image: &rekognition.Image{
			//Bytes:    imageBytes,
			S3Object: &rekognition.S3Object{
				Bucket: aws.String("sample-images-wcy"),
				Name:   aws.String("Tom-Hardy-base.jpg"),
			},
		},
	}

	result, err := svc.IndexFaces(input)
	if err != nil {
		errCode, errMsg := awsRekognitionErrorHandler(err)
		return nil, status.Error(codes.InvalidArgument, "Recognition-Create: "+errCode+": "+errMsg)
	}

	// check the image contains a valid human face where confidence > enrollConfidenceThreshold
	if *result.FaceRecords[0].Face.Confidence < enrollConfidenceThreshold {
		return nil, status.Error(codes.InvalidArgument, "Recognition-Create: No face detected")
	}

	tempEmp := employee{
		empID:              uuid.New().String(),
		empCardID:          emp.GetEmpCardID(),
		empFirst:           emp.GetEmpFirst(),
		empLast:            emp.GetEmpLast(),
		empEmail:           emp.GetEmpEmail(),
		empExternalImageID: empExternalImageID,
	}

	// persist employee data into DB
	CallEmployeeDBServiceToCreateEmp(ctx, tempEmp, employeedbAddress)

	return &recognitionpb.CreateEmployeeResponse{
		EmpFirst: emp.GetEmpFirst(),
		EmpLast:  emp.GetEmpLast(),
	}, nil
}

func (s *recognitionServiceServer) SearchEmployeeByFace(ctx context.Context, req *recognitionpb.SearchEmployeeByFaceRequest) (*recognitionpb.SearchEmployeeResponse, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Recognition-Search: Error when create session %v", err)
	}

	svc := rekognition.New(sess)

	// TODO: get face from req and send to AWS rekognition to get the corresponding email address. Look up the email address to check existence.
	// imageBytes := getImageBytesFromBase64String(req.GetFace().EmpFace)
	input := &rekognition.SearchFacesByImageInput{
		CollectionId:       aws.String("testPhotos"),
		FaceMatchThreshold: aws.Float64(searchConfidenceThreshold),
		Image: &rekognition.Image{
			// Bytes:    imageBytes,
			S3Object: &rekognition.S3Object{
				Bucket: aws.String("sample-images-wcy"),
				Name:   aws.String("Tom-Hardy1.jpg"),
			},
		},
		MaxFaces: aws.Int64(5),
	}

	result, err := svc.SearchFacesByImage(input)
	if err != nil {
		errCode, errMsg := awsRekognitionErrorHandler(err)
		return nil, status.Error(codes.InvalidArgument, "Recognition-Search: "+errCode+": "+errMsg)
	}

	if len(result.FaceMatches) == 0 {
		return nil, status.Error(codes.NotFound, "Recognition-Search: Employee doesn't exist")
	}

	empExternalImageID := *result.FaceMatches[0].Face.ExternalImageId
	empEmail := strings.ReplaceAll(empExternalImageID, "AT", "@")

	emp, err := CallEmployeeDBServiceToSearchFace(ctx, empEmail, employeedbAddress)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Recognition-SearchFace: Error when connecting employee db: %v", err)
	}

	SignInEmployee(ctx, emp.GetEmpEmail(), emp.GetEmpCardID())

	return &recognitionpb.SearchEmployeeResponse{
		EmpID:     emp.GetEmpID(),
		EmpCardID: emp.GetEmpCardID(),
		EmpFirst:  emp.GetEmpFirst(),
		EmpLast:   emp.GetEmpLast(),
		EmpEmail:  emp.GetEmpEmail(),
	}, nil
}

func (s *recognitionServiceServer) SearchEmployeeByCard(ctx context.Context, req *recognitionpb.SearchEmployeeByCardRequest) (*recognitionpb.SearchEmployeeResponse, error) {

	empCardID := req.GetEmpCardID()

	emp, err := CallEmployeeDBServiceToSearchCard(ctx, empCardID, employeedbAddress)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Recognition-SearchCard: Error when connecting employee db: %v", err)
	}

	SignInEmployee(ctx, emp.GetEmpEmail(), emp.GetEmpCardID())

	return &recognitionpb.SearchEmployeeResponse{
		EmpID:     emp.GetEmpID(),
		EmpCardID: emp.GetEmpCardID(),
		EmpFirst:  emp.GetEmpFirst(),
		EmpLast:   emp.GetEmpLast(),
		EmpEmail:  emp.GetEmpEmail(),
	}, nil
}

func SignInEmployee(ctx context.Context, empEmail string, empCardID string) error {

	if empEmail == "" || empCardID == "" {
		return status.Error(codes.InvalidArgument, "Recognition-SignIn: employee email and card ID cannot be empty")
	} else if !validateEmailFormat(empEmail) {
		return status.Error(codes.InvalidArgument, "Recognition-SignIn: Invalid email format")
	}

	err := CallEmployeeDBServiceToSignInEmp(ctx, empEmail, empCardID, employeedbAddress)
	if err != nil {
		return status.Errorf(codes.Internal, "Recognition-SignIn: Error when connecting employee db: %v", err)
	}

	return nil
}

//func (s *recognitionServiceServer) SignInEmployee(ctx context.Context, req *recognitionpb.SignInEmployeeRequest) (*recognitionpb.SignInEmployeeResponse, error) {
//
//	empEmail := req.GetEmpEmail()
//	empCardID := req.GetEmpCardID()
//
//	if empEmail == "" || empCardID == "" {
//		return nil, status.Error(codes.InvalidArgument, "Recognition-SignIn: employee email and card ID cannot be empty")
//	} else if !validateEmailFormat(empEmail) {
//		return nil, status.Error(codes.InvalidArgument, "Recognition-SignIn: Invalid email format")
//	}
//
//	signInMessage, err := CallEmployeeDBServiceToSignInEmp(ctx, empEmail, empCardID, employeedbAddress)
//	if err != nil {
//		return nil, status.Errorf(codes.Internal, "Recognition-SignIn: Error when connecting employee db: %v", err)
//	}
//
//	return &recognitionpb.SignInEmployeeResponse{
//		SignInMessage: signInMessage,
//	}, nil
//}

func (s *recognitionServiceServer) GetAllEmployees(ctx context.Context, req *empty.Empty) (*recognitionpb.GetAllEmployeesResponse, error) {

	employeeList, err := CallEmployeeDBServiceToGetAllEmp(ctx, employeedbAddress)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Recognition-GetAllEmp: Error when connecting employee db: %v", err)
	}

	var employeeResponseList []*recognitionpb.Employee
	for _, v := range employeeList {
		tempEmp := &recognitionpb.Employee{
			EmpID:              v.GetEmpID(),
			EmpCardID:          v.GetEmpCardID(),
			EmpFirst:           v.GetEmpFirst(),
			EmpLast:            v.GetEmpLast(),
			EmpEmail:           v.GetEmpEmail(),
			EmpExternalImageID: v.GetEmpExternalImageID(),
			AttendanceStatus:   v.GetAttendanceStatus(),
			SignInTime:         v.GetSignInTime(),
			SignOutTime:        v.GetSignOutTime(),
		}

		employeeResponseList = append(employeeResponseList, tempEmp)
	}

	return &recognitionpb.GetAllEmployeesResponse{
		Employee: employeeResponseList,
	}, nil
}

func main() {
	fmt.Println("Hello from Recognition Service")

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{}
	s := grpc.NewServer(opts...)
	recognitionpb.RegisterRecognitionServiceServer(s, &recognitionServiceServer{})
	log.Fatal(s.Serve(lis))
}
