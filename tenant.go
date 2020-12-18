package nsq

import "gitlab.qima-inc.com/paas/yz-go-libs/yzapp"

const T_V1 = "v1"

type Tenant struct {
	//tenant 版本号
	Version string `json:"version"`
	//用户名称，对应应用名
	Name string `json:"name"`
	//客户端保留对应的特征值, application labels
	ExtraPayload map[string]string `json:"extra_payload"`
}

func buildTenant() (*Tenant, error) {
	app, err := yzapp.NewDefault()
	if err != nil {
		return nil, err
	}

	t := &Tenant{
		Version: T_V1,
	}

	t.Name = app.AppName
	t.ExtraPayload = app.AppLabels()
	return t, nil
}
