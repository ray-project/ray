package org.ray.streaming.benchmark;

import java.util.HashMap;
import org.ray.streaming.api.function.impl.KeyFunction;
import org.ray.streaming.util.Record3;

public class MyKeyBySelector implements KeyFunction<Record3, Integer> {

  public HashMap<String, Integer> campaignMap = new HashMap<>();

  public MyKeyBySelector() {
    campaignMap.put("33a68ccc-be1f-44f3-9dc0-bb26eb02e6bc", 1);
    campaignMap.put("b4bb9671-3902-44d5-a2d7-343090a92b60", 2);
    campaignMap.put("03045d6a-2734-4bd9-af9e-5d12301a3703", 3);
    campaignMap.put("11a976bc-4a03-40ca-876f-47887910a8d5", 4);
    campaignMap.put("1a8dc2c3-d864-4533-bb8c-92d76045c878", 5);
    campaignMap.put("8b1a9588-700f-4213-8d7b-1605e9c95ad4", 6);
    campaignMap.put("35b3c31d-1709-4aa7-a0e8-cf6df8c60b81", 7);
    campaignMap.put("028c716b-71ef-4958-9605-d4c866535ccb", 8);
    campaignMap.put("4a32877e-c8d1-4edf-b731-e60410c58683", 9);
    campaignMap.put("2265e69c-46a1-439d-b9cf-a78cdd97a858", 10);
    campaignMap.put("8189fb5b-80ac-471d-914c-0833d35ee577", 11);
    campaignMap.put("1dacbffe-36f6-4ed4-82f6-d55adf3d9a7d", 12);
    campaignMap.put("99d87fde-b08c-480b-9120-af8d10fa9d38", 13);
    campaignMap.put("32581daa-1773-46f7-a6aa-807a0118ab07", 14);
    campaignMap.put("f71a4f57-93dd-42c3-acd3-215ef014a200", 15);
    campaignMap.put("9c75c73c-8acb-4caf-a06d-af55ea747d14", 16);
    campaignMap.put("5dce0e5d-37f2-445b-9182-74da96e9d5e3", 17);
    campaignMap.put("4f0c2bfe-6a34-4be0-a2b7-72e8f74148aa", 18);
    campaignMap.put("12ac8bc5-3bf2-4a7f-ab38-c0be3aedce00", 19);
    campaignMap.put("bafee919-5a28-4cc1-8ae3-5afd962fc7fc", 20);
    campaignMap.put("9df60fad-1b9e-4397-a405-564184d6d098", 21);
    campaignMap.put("8924bfe3-99a7-41db-88f1-d3ef8c28d230", 22);
    campaignMap.put("064155dc-ae04-422f-b2af-65bdbb5be08d", 23);
    campaignMap.put("27fce217-fc82-437a-bd5c-ce1ba743700a", 24);
    campaignMap.put("fdb75750-5839-4f5d-bb75-3f464a784634", 25);
    campaignMap.put("cc0ebb22-0266-4e67-b2b8-f1115ecf512e", 26);
    campaignMap.put("71f14533-75ae-426b-a113-524dfff552b4", 27);
    campaignMap.put("b94105fc-9f93-43d8-b1aa-d51571ccf852", 28);
    campaignMap.put("c9caaaa3-a707-4fd9-ace9-9efd93a19edf", 29);
    campaignMap.put("3eae92ed-ae9c-4cb3-8470-6d627761057d", 30);
    campaignMap.put("b3b34e4e-02d4-42d6-a6aa-69e6facd3a53", 31);
    campaignMap.put("582a995a-a4cb-4642-9f8f-834e1e4018ac", 32);
    campaignMap.put("94725e81-08cc-4c60-999e-98d3c5dbed6c", 33);
    campaignMap.put("2a92aa37-621d-4bfa-a5b3-7506af2c747e", 34);
    campaignMap.put("ef26285e-3db8-441c-9d6c-68fb55397c9a", 35);
    campaignMap.put("3da46da0-0fa5-43ef-b927-db56891853c4", 36);
    campaignMap.put("daba3fd2-4591-4a66-ad22-7b59c4b27f73", 37);
    campaignMap.put("092e3387-71d9-4861-a56b-bfd87c122523", 38);
    campaignMap.put("276682d9-e1e2-4883-8de1-1b2b4d5980a9", 39);
    campaignMap.put("59723517-4141-4a29-9a34-81d0a79c8012", 40);
    campaignMap.put("05e78172-f399-4944-b78b-a226d73a60f0", 41);
    campaignMap.put("c8d13f93-99c1-47ef-bc17-51843bb10da6", 42);
    campaignMap.put("60ec9462-7a21-458f-ad40-fab94fb4edea", 43);
    campaignMap.put("a5989f9a-e3db-40da-a0c3-80330fd0d8a5", 44);
    campaignMap.put("7cb1c8ea-dd09-4cf5-9b86-6a0fb762005c", 45);
    campaignMap.put("11491213-ca7d-4528-8e2d-5d1c4ce0cf37", 46);
    campaignMap.put("14cf58e4-bda0-4b76-8c3e-8a3ccf99e5c1", 47);
    campaignMap.put("fa004c07-042d-4d61-9e07-62bf9e6b0dd0", 48);
    campaignMap.put("95507e81-fba5-4c24-857c-4f9000693d65", 49);
    campaignMap.put("6aa72028-c578-41bc-975a-211ec421ea29", 50);
    campaignMap.put("9a3d2917-93bb-42a6-9afb-fedbe34d3290", 51);
    campaignMap.put("66efe2a5-4e21-4976-a559-c606dc83ab8d", 52);
    campaignMap.put("c781bcbe-be65-4305-b532-f24a6727b3a3", 53);
    campaignMap.put("c4ab2371-b61f-43f6-9427-60ca556e653e", 54);
    campaignMap.put("9fb2c390-1edf-41e3-bb73-0f393e4b87cf", 55);
    campaignMap.put("aa363a08-7f04-4646-87d4-cf1ed9a81169", 56);
    campaignMap.put("2c9a2e1e-3d67-4419-93b0-0d3d7faa301c", 57);
    campaignMap.put("61f86b89-0ef6-4bef-8ce7-b9c8fa11cb33", 58);
    campaignMap.put("d3881c04-c2ba-4f33-969f-4255e89bf5bd", 59);
    campaignMap.put("a681ef51-3174-46f7-9b2d-808a7bfdabd5", 60);
    campaignMap.put("a9fe783a-62fc-454c-bb52-b49fa0eab32e", 61);
    campaignMap.put("d0b42cb8-778a-497b-a09c-47c3a5b8130f", 62);
    campaignMap.put("a4f281c9-6b7d-45e1-be1c-a8fa3bbfba1c", 63);
    campaignMap.put("1a1c8436-00b2-4763-b7cb-1e9b8b07ea3c", 64);
    campaignMap.put("9371da02-ccd9-47f9-83f2-1cb29948ee38", 65);
    campaignMap.put("803f8149-2804-44bb-8eef-d4ec508f0bb7", 66);
    campaignMap.put("40b4bed6-8483-4ce5-9d44-937f8bace50e", 67);
    campaignMap.put("122c001b-7441-419c-a999-a19e35c7853f", 68);
    campaignMap.put("4e6a5991-dd78-4573-8341-9d171bba3ea4", 69);
    campaignMap.put("945e4c33-961d-402e-b1e9-013799e1e766", 70);
    campaignMap.put("953aa0a3-ac85-4de1-9d7b-ec1ea724e11f", 71);
    campaignMap.put("c570d292-2d9f-4a89-9cde-9955cd171eec", 72);
    campaignMap.put("9edda783-2c07-49d5-85e2-218334f0bb20", 73);
    campaignMap.put("021de342-72ce-4fab-81df-0efcc948ad2b", 74);
    campaignMap.put("226f75df-6edf-442e-a8a6-cff02b80a20b", 75);
    campaignMap.put("dd450219-c212-4fc7-8a3b-64709c200e0d", 76);
    campaignMap.put("bab45763-811b-4ce1-9129-454a0bbc07f0", 77);
    campaignMap.put("9d45a04e-85b8-4ae1-b231-4f7857b29a72", 78);
    campaignMap.put("6786fcb3-0c68-49da-92e4-851c0164729c", 79);
    campaignMap.put("ccef50f6-1eca-4dd0-8b66-1a53fa1bf8b6", 80);
    campaignMap.put("2babec18-b440-48d1-8ce6-c1d0f266b294", 81);
    campaignMap.put("744fc65a-e68f-4f03-920e-52d85261b32a", 82);
    campaignMap.put("0502946f-d4fc-41b2-87fd-7d56d5afdfd5", 83);
    campaignMap.put("1e2b6c6c-2e0c-4474-9198-fd5138339996", 84);
    campaignMap.put("66fbbb9b-df2d-42d7-aaba-a3962d8aa224", 85);
    campaignMap.put("d570465b-d929-41b4-8907-0f5275c9baa1", 86);
    campaignMap.put("8fb66bca-3f25-4efe-a50b-70689317bf61", 87);
    campaignMap.put("60b60c76-8dde-4e8b-b45a-74fabe3a38bc", 88);
    campaignMap.put("068bfd7e-7eaa-42be-82b7-bd817cff843c", 89);
    campaignMap.put("e96c4016-c4db-4f5e-955e-48f4ef26482c", 90);
    campaignMap.put("c640cda0-5984-4b80-9d59-81b9f393a402", 91);
    campaignMap.put("8a5bb8e1-f6e9-47ec-bb88-6f5dfa9f61fe", 92);
    campaignMap.put("613b0932-6ce8-4bc5-8f07-f2006296c9e1", 93);
    campaignMap.put("9a4614a9-fff6-44a8-97d2-3737b3db3591", 94);
    campaignMap.put("c44ada65-d2e0-4e0c-8b6f-52e05f11713c", 95);
    campaignMap.put("4d379ee5-c1e2-4a86-93b3-00192b69a291", 96);
    campaignMap.put("eebefe87-6258-4077-89d5-ba273c63ec94", 97);
    campaignMap.put("5c56f348-4b01-4ed1-a689-6ed01f109371", 98);
    campaignMap.put("1d052d7b-6952-4a0d-a67a-31bb1816ae6f", 99);
    campaignMap.put("ea447920-9967-4110-97c6-318d96295b9e", 100);
  }


  @Override
  public Integer keyBy(Record3 record3) {
    return campaignMap.get(record3.getF0());
  }
}
