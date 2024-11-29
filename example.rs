use std::fs;
use std::path::Path;
use std::process::Command;

use dotenvy::dotenv;

/// Function to retrieve objects from the bucket.
pub fn retrieve_objects(bucket_name: &str, namespace: &str, local_dir: &str) -> Result<(), String> {
    let output = Command::new("oci")
        .args([
            "os",
            "object",
            "bulk-download",
            "--bucket-name",
            bucket_name,
            "--namespace-name",
            namespace,
            "--download-dir",
            local_dir,
        ])
        .output()
        .map_err(|e| format!("Failed to invoke OCI CLI: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "Error retrieving objects: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    println!("Successfully downloaded objects to {}", local_dir);
    Ok(())
}

/// Function to upload files or directories to the bucket.
pub fn upload_objects(bucket_name: &str, namespace: &str, local_path: &str) -> Result<(), String> {
    let path = Path::new(local_path);
    if !path.exists() {
        return Err(format!("Path does not exist: {}", local_path));
    }

    let output = if path.is_dir() {
        Command::new("oci")
            .args([
                "os",
                "object",
                "bulk-upload",
                "--bucket-name",
                bucket_name,
                "--namespace-name",
                namespace,
                "--src-dir",
                local_path,
            ])
            .output()
            .map_err(|e| format!("Failed to invoke OCI CLI: {}", e))?
    } else {
        Command::new("oci")
            .args([
                "os",
                "object",
                "put",
                "--bucket-name",
                bucket_name,
                "--namespace-name",
                namespace,
                "--file",
                local_path,
                "--name",
                path.file_name().unwrap().to_str().unwrap(),
            ])
            .output()
            .map_err(|e| format!("Failed to invoke OCI CLI: {}", e))?
    };

    if !output.status.success() {
        return Err(format!(
            "Error uploading objects: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    println!(
        "Successfully uploaded {} to bucket {}",
        local_path, bucket_name
    );
    Ok(())
}

/// Function to delete objects from the bucket.
pub fn delete_object(bucket_name: &str, namespace: &str, object_name: &str) -> Result<(), String> {
    let output = Command::new("oci")
        .args([
            "os",
            "object",
            "delete",
            "--bucket-name",
            bucket_name,
            "--namespace-name",
            namespace,
            "--name",
            object_name,
            "--force",
        ])
        .output()
        .map_err(|e| format!("Failed to invoke OCI CLI: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "Error deleting object: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    println!(
        "Successfully deleted object {} from bucket {}",
        object_name, bucket_name
    );
    Ok(())
}

/// Example usage
fn main() {
    dotenv().ok();

    let bucket_name = std::env::var_os("BUCKET_NAME")
        .expect("Bucket name has not been set.")
        .into_string()
        .unwrap();
    let namespace = std::env::var_os("NAMESPACE")
        .expect("Namespace has not been set.")
        .into_string()
        .unwrap();
    let test_dir = "./data";

    // Retrieve objects
    // if let Err(e) = retrieve_objects(&bucket_name, &namespace, "./work_dir") {
    //     eprintln!("Retrieve failed: {}", e);
    // }

    // // Upload files or directories
    // if let Err(e) = upload_objects(&bucket_name, &namespace, test_dir) {
    //     eprintln!("Upload failed: {}", e);
    // }

    // Delete a specific object
    if let Err(e) = delete_object(&bucket_name, &namespace, "hello.txt") {
        eprintln!("Delete failed: {}", e);
    }
}
